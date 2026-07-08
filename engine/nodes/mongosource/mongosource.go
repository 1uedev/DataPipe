// Package mongosource implements the "mongo-source" node (CON-520 NoSQL:
// MongoDB find/aggregate; change streams are P2, not implemented — see
// TODO.md): one-shot or periodic query, one datagram per matching document.
package mongosource

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/mongoshared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["once", "periodic"], "description": "Run the query once, or repeatedly on an interval." },
		"collection": { "type": "string" },
		"operation": { "type": "string", "enum": ["find", "aggregate"], "default": "find" },
		"filter": { "type": "object", "description": "Query filter document for \"find\" (default {}, i.e. all documents)." },
		"pipeline": { "type": "array", "items": { "type": "object" }, "description": "Aggregation pipeline stages for \"aggregate\"." },
		"intervalMs": { "type": "integer", "minimum": 1, "description": "Period between queries in mode \"periodic\"." }
	},
	"required": ["mode", "collection"]
}`

func init() {
	flow.Register("mongo-source", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "MongoDB Source",
		Category:     flow.CategorySource,
		Description:  "One-shot or periodic find/aggregate query, one datagram per document (CON-520).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "mongo-source" node's "config" object.
type Config struct {
	Mode       string            `json:"mode"`
	Collection string            `json:"collection"`
	Operation  string            `json:"operation,omitempty"` // default "find"
	Filter     json.RawMessage   `json:"filter,omitempty"`
	Pipeline   []json.RawMessage `json:"pipeline,omitempty"`
	IntervalMs int               `json:"intervalMs,omitempty"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "mongo-source" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Collection == "" {
		return nil, fmt.Errorf("mongo-source: collection is required")
	}
	switch cfg.Mode {
	case "once":
	case "periodic":
		if cfg.IntervalMs <= 0 {
			return nil, fmt.Errorf("mongo-source: intervalMs must be positive in mode \"periodic\"")
		}
	default:
		return nil, fmt.Errorf("mongo-source: mode must be \"once\" or \"periodic\", got %q", cfg.Mode)
	}
	if cfg.Operation == "" {
		cfg.Operation = "find"
	}
	if cfg.Operation != "find" && cfg.Operation != "aggregate" {
		return nil, fmt.Errorf("mongo-source: operation must be \"find\" or \"aggregate\", got %q", cfg.Operation)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	db, err := mongoshared.Connect(ctx)
	if err != nil {
		return err
	}

	if err := n.runQuery(ctx, db, emit); err != nil {
		return err
	}
	if n.cfg.Mode == "once" {
		return nil
	}

	ticker := time.NewTicker(time.Duration(n.cfg.IntervalMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := n.runQuery(ctx, db, emit); err != nil {
				return err
			}
		}
	}
}

func (n *node) runQuery(ctx context.Context, db *mongo.Database, emit func(string, datagram.Datagram) error) error {
	coll := db.Collection(n.cfg.Collection)

	var cur *mongo.Cursor
	var err error
	if n.cfg.Operation == "aggregate" {
		pipeline := make(bson.A, 0, len(n.cfg.Pipeline))
		for _, stage := range n.cfg.Pipeline {
			var m bson.M
			if err := bson.UnmarshalExtJSON(stage, false, &m); err != nil {
				return fmt.Errorf("mongo-source: parsing pipeline stage: %w", err)
			}
			pipeline = append(pipeline, m)
		}
		cur, err = coll.Aggregate(ctx, pipeline)
	} else {
		filter, ferr := decodeFilter(n.cfg.Filter)
		if ferr != nil {
			return ferr
		}
		cur, err = coll.Find(ctx, filter)
	}
	if err != nil {
		return fmt.Errorf("mongo-source: query: %w", err)
	}
	defer func() { _ = cur.Close(ctx) }()

	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return fmt.Errorf("mongo-source: decoding document: %w", err)
		}
		value := mongoshared.NormalizeValue(doc)
		d := datagram.New(datagram.Source{NodeID: "mongo-source"}, datagram.Payload{Value: value})
		if err := emit("out", d); err != nil {
			return err
		}
	}
	return cur.Err()
}

func decodeFilter(raw json.RawMessage) (bson.M, error) {
	if len(raw) == 0 {
		return bson.M{}, nil
	}
	var m bson.M
	if err := bson.UnmarshalExtJSON(raw, false, &m); err != nil {
		return nil, fmt.Errorf("mongo-source: parsing filter: %w", err)
	}
	return m, nil
}
