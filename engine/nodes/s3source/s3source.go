// Package s3source implements the "s3-source" node (CON-400's S3-compatible
// object storage clause / Increment 10 MVP catalog "S3 files"): one-shot or
// periodic listing of new objects under a prefix, parsed with the same
// engine/nodes/recordformat readers used by "file-watch", with an optional
// post-action.
package s3source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/recordformat"
	"github.com/1uedev/DataPipe/engine/nodes/s3shared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["once", "periodic"] },
		"prefix": { "type": "string", "description": "Only objects whose key starts with this prefix." },
		"format": { "type": "string", "enum": ["csv", "tsv", "json", "jsonl", "xml", "xlsx", "raw"] },
		"csv": {
			"type": "object",
			"properties": {
				"delimiter": { "type": "string" },
				"hasHeader": { "type": "boolean" },
				"encoding": { "type": "string", "enum": ["utf-8", "latin1"] }
			}
		},
		"excel": {
			"type": "object",
			"properties": { "sheetName": { "type": "string" }, "hasHeader": { "type": "boolean" } }
		},
		"jsonRoot": { "type": "string" },
		"xmlRecordElement": { "type": "string" },
		"malformedRowPolicy": { "type": "string", "enum": ["fail", "skip"] },
		"intervalMs": { "type": "integer", "minimum": 1, "description": "Period between listings in mode \"periodic\"." },
		"postAction": { "type": "string", "enum": ["keep", "delete", "move"], "description": "What to do with an object after it's read (default keep)." },
		"moveToPrefix": { "type": "string", "description": "Destination prefix for postAction \"move\" (copy then delete the original)." }
	},
	"required": ["mode", "format"]
}`

func init() {
	flow.Register("s3-source", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "S3 Source",
		Category:     flow.CategorySource,
		Description:  "List and parse new objects under a prefix (S3-compatible object storage).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "s3-source" node's "config" object.
type Config struct {
	Mode               string                   `json:"mode"`
	Prefix             string                   `json:"prefix,omitempty"`
	Format             string                   `json:"format"`
	CSV                recordformat.CSVConfig   `json:"csv,omitempty"`
	Excel              recordformat.ExcelConfig `json:"excel,omitempty"`
	JSONRoot           string                   `json:"jsonRoot,omitempty"`
	XMLRecordElement   string                   `json:"xmlRecordElement,omitempty"`
	MalformedRowPolicy string                   `json:"malformedRowPolicy,omitempty"`
	IntervalMs         int                      `json:"intervalMs,omitempty"`
	PostAction         string                   `json:"postAction,omitempty"`
	MoveToPrefix       string                   `json:"moveToPrefix,omitempty"`
}

type node struct {
	cfg  Config
	seen map[string]bool
}

// New is the flow.Factory for the "s3-source" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	switch cfg.Mode {
	case "once":
	case "periodic":
		if cfg.IntervalMs <= 0 {
			return nil, fmt.Errorf("s3-source: intervalMs must be positive in mode \"periodic\"")
		}
	default:
		return nil, fmt.Errorf("s3-source: mode must be \"once\" or \"periodic\", got %q", cfg.Mode)
	}
	switch cfg.Format {
	case "csv", "tsv", "json", "jsonl", "xml", "xlsx", "raw":
	default:
		return nil, fmt.Errorf("s3-source: unknown format %q", cfg.Format)
	}
	if cfg.PostAction == "move" && cfg.MoveToPrefix == "" {
		return nil, fmt.Errorf("s3-source: moveToPrefix is required for postAction \"move\"")
	}
	return &node{cfg: cfg, seen: map[string]bool{}}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	resolved, err := s3shared.Connect(ctx)
	if err != nil {
		return err
	}

	if err := n.scan(ctx, resolved, emit); err != nil {
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
			if err := n.scan(ctx, resolved, emit); err != nil {
				return err
			}
		}
	}
}

func (n *node) scan(ctx context.Context, resolved s3shared.Resolved, emit func(string, datagram.Datagram) error) error {
	var token *string
	for {
		out, err := resolved.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(resolved.Bucket),
			Prefix:            aws.String(n.cfg.Prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return fmt.Errorf("s3-source: listing objects: %w", err)
		}
		for _, obj := range out.Contents {
			key := aws.ToString(obj.Key)
			if n.seen[key] {
				continue
			}
			n.seen[key] = true
			if err := n.processObject(ctx, resolved, key, emit); err != nil {
				return err
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			return nil
		}
		token = out.NextContinuationToken
	}
}

func (n *node) processObject(ctx context.Context, resolved s3shared.Resolved, key string, emit func(string, datagram.Datagram) error) error {
	get, err := resolved.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(resolved.Bucket), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("s3-source: getting object %q: %w", key, err)
	}
	raw, err := io.ReadAll(get.Body)
	_ = get.Body.Close()
	if err != nil {
		return fmt.Errorf("s3-source: reading object %q: %w", key, err)
	}

	records, err := recordformat.ParseRecords(n.cfg.Format, raw, recordformat.Options{
		CSV: n.cfg.CSV, Excel: n.cfg.Excel, JSONRoot: n.cfg.JSONRoot,
		XMLRecordElement: n.cfg.XMLRecordElement, MalformedRowPolicy: n.cfg.MalformedRowPolicy,
	})
	if err != nil {
		return fmt.Errorf("s3-source: parsing object %q: %w", key, err)
	}
	for _, v := range records {
		d := datagram.New(datagram.Source{NodeID: "s3-source", Origin: key}, datagram.Payload{Value: v})
		if err := emit("out", d); err != nil {
			return err
		}
	}
	return n.applyPostAction(ctx, resolved, key)
}

func (n *node) applyPostAction(ctx context.Context, resolved s3shared.Resolved, key string) error {
	switch n.cfg.PostAction {
	case "", "keep":
		return nil
	case "delete":
		_, err := resolved.Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(resolved.Bucket), Key: aws.String(key)})
		return err
	case "move":
		newKey := n.cfg.MoveToPrefix + key
		if _, err := resolved.Client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(resolved.Bucket),
			Key:        aws.String(newKey),
			CopySource: aws.String(resolved.Bucket + "/" + key),
		}); err != nil {
			return fmt.Errorf("copying to %q: %w", newKey, err)
		}
		_, err := resolved.Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(resolved.Bucket), Key: aws.String(key)})
		return err
	default:
		return fmt.Errorf("unknown postAction %q", n.cfg.PostAction)
	}
}
