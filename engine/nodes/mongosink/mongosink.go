// Package mongosink implements the "mongo-sink" node (SNK-200 NoSQL:
// MongoDB insert/update/upsert). A datagram whose payload is an array
// writes a batch of documents; a single-object payload is a batch of one.
package mongosink

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/mongoshared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["insert", "update", "upsert"] },
		"collection": { "type": "string" },
		"filterFields": { "type": "array", "items": { "type": "string" }, "description": "Payload fields used as the match filter for update/upsert." }
	},
	"required": ["mode", "collection"]
}`

func init() {
	flow.Register("mongo-sink", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "MongoDB Sink",
		Category:     flow.CategoryProcessor,
		Description:  "Insert/update/upsert documents (SNK-200).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "mongo-sink" node's "config" object.
type Config struct {
	Mode         string   `json:"mode"`
	Collection   string   `json:"collection"`
	FilterFields []string `json:"filterFields,omitempty"`
}

type node struct {
	cfg Config

	connectOnce sync.Once
	db          *mongo.Database
	connectErr  error
}

// New is the flow.Factory for the "mongo-sink" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Collection == "" {
		return nil, fmt.Errorf("mongo-sink: collection is required")
	}
	switch cfg.Mode {
	case "insert":
	case "update", "upsert":
		if len(cfg.FilterFields) == 0 {
			return nil, fmt.Errorf("mongo-sink: filterFields is required for mode %q", cfg.Mode)
		}
	default:
		return nil, fmt.Errorf("mongo-sink: mode must be \"insert\", \"update\", or \"upsert\", got %q", cfg.Mode)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) connect(ctx context.Context) (*mongo.Database, error) {
	n.connectOnce.Do(func() { n.db, n.connectErr = mongoshared.Connect(ctx) })
	return n.db, n.connectErr
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	db, err := n.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("mongo-sink: %w", err)
	}
	coll := db.Collection(n.cfg.Collection)

	docs := batchDocs(in.Payload.Value)
	if len(docs) == 0 {
		return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
	}

	for _, doc := range docs {
		if err := n.writeOne(ctx, coll, doc); err != nil {
			return nil, fmt.Errorf("mongo-sink: %w", err)
		}
	}
	return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
}

func (n *node) writeOne(ctx context.Context, coll *mongo.Collection, doc map[string]any) error {
	switch n.cfg.Mode {
	case "insert":
		_, err := coll.InsertOne(ctx, doc)
		return err
	case "update", "upsert":
		filter := bson.M{}
		update := bson.M{}
		for k, v := range doc {
			if contains(n.cfg.FilterFields, k) {
				filter[k] = v
			} else {
				update[k] = v
			}
		}
		opts := options.UpdateOne().SetUpsert(n.cfg.Mode == "upsert")
		_, err := coll.UpdateOne(ctx, filter, bson.M{"$set": update}, opts)
		return err
	default:
		return fmt.Errorf("unknown mode %q", n.cfg.Mode)
	}
}

func contains(fields []string, name string) bool {
	for _, f := range fields {
		if f == name {
			return true
		}
	}
	return false
}

// batchDocs normalizes a datagram payload into one or more documents,
// mirroring engine/nodes/sqlsink's batchRows.
func batchDocs(value any) []map[string]any {
	switch v := value.(type) {
	case []any:
		docs := make([]map[string]any, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				docs = append(docs, m)
			}
		}
		return docs
	case map[string]any:
		return []map[string]any{v}
	default:
		return nil
	}
}
