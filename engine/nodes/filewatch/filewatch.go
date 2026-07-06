// Package filewatch implements the "file-watch" node (CON-400 File
// Watcher + CON-410 File Readers, CSV/TSV and JSON/JSON-Lines slices):
// watches a directory for new/modified files matching a pattern, waits for
// each file to stop growing (a "stability check"), parses it into records,
// emits one datagram per record (or one batch datagram), and applies a
// post-action (keep / mark / move / rename / delete). Excel, XML, Parquet,
// and fixed-width readers are P2/deferred — see TODO.md.
package filewatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

// DefaultStabilityMs is how long a file's size must stay unchanged before
// it's considered fully written (CON-400 "stability check").
const DefaultStabilityMs = 500

const configSchema = `{
	"type": "object",
	"properties": {
		"directory": { "type": "string", "description": "Directory to watch." },
		"pattern": { "type": "string", "description": "Glob pattern matched against the file name, e.g. \"*.csv\"." },
		"recursive": { "type": "boolean", "description": "Also watch subdirectories." },
		"format": { "type": "string", "enum": ["csv", "tsv", "json", "jsonl", "raw"] },
		"csv": {
			"type": "object",
			"properties": {
				"delimiter": { "type": "string", "description": "Single-character field delimiter (default \",\" or \"\\t\" for tsv)." },
				"hasHeader": { "type": "boolean" },
				"encoding": { "type": "string", "enum": ["utf-8", "latin1"] }
			}
		},
		"jsonRoot": { "type": "string", "description": "\".\"-path to an array within the JSON document to stream element-by-element; empty streams the whole document as one record." },
		"emit": { "type": "string", "enum": ["perRecord", "batch"], "description": "One datagram per record (default) or a single datagram carrying every record." },
		"stabilityMs": { "type": "integer", "minimum": 0, "description": "How long the file size must stay unchanged before reading it (default 500)." },
		"malformedRowPolicy": { "type": "string", "enum": ["fail", "skip"], "description": "What to do with a row/line that fails to parse (default fail)." },
		"postAction": {
			"type": "object",
			"properties": {
				"action": { "type": "string", "enum": ["keep", "markerFile", "move", "rename", "delete"] },
				"moveTo": { "type": "string", "description": "Destination directory for action \"move\"." },
				"renamePattern": { "type": "string", "description": "For action \"rename\": \"{{name}}\"/\"{{ext}}\" placeholders, e.g. \"{{name}}.done{{ext}}\"." }
			}
		}
	},
	"required": ["directory", "pattern", "format"]
}`

func init() {
	flow.Register("file-watch", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "File Watch",
		Category:     flow.CategorySource,
		Description:  "Watches a directory for files matching a pattern and emits parsed records (CON-400/410: CSV/TSV, JSON, JSON Lines).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// PostAction is the "postAction" config object.
type PostAction struct {
	Action        string `json:"action,omitempty"` // "keep" (default) | "markerFile" | "move" | "rename" | "delete"
	MoveTo        string `json:"moveTo,omitempty"`
	RenamePattern string `json:"renamePattern,omitempty"`
}

// Config is the "file-watch" node's "config" object.
type Config struct {
	Directory          string     `json:"directory"`
	Pattern            string     `json:"pattern"`
	Recursive          bool       `json:"recursive,omitempty"`
	Format             string     `json:"format"`
	CSV                CSVConfig  `json:"csv,omitempty"`
	JSONRoot           string     `json:"jsonRoot,omitempty"`
	Emit               string     `json:"emit,omitempty"` // "perRecord" (default) | "batch"
	StabilityMs        int        `json:"stabilityMs,omitempty"`
	MalformedRowPolicy string     `json:"malformedRowPolicy,omitempty"`
	PostAction         PostAction `json:"postAction,omitempty"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "file-watch" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Directory == "" {
		return nil, fmt.Errorf("file-watch: directory is required")
	}
	if cfg.Pattern == "" {
		return nil, fmt.Errorf("file-watch: pattern is required")
	}
	switch cfg.Format {
	case "csv", "tsv", "json", "jsonl", "raw":
	default:
		return nil, fmt.Errorf("file-watch: unknown format %q", cfg.Format)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("file-watch: %w", err)
	}
	defer func() { _ = watcher.Close() }()

	if err := n.addWatches(watcher); err != nil {
		return fmt.Errorf("file-watch: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			n.handleEvent(ctx, watcher, event, emit)
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			slog.Warn("file-watch: watcher error", "error", err)
		}
	}
}

func (n *node) addWatches(watcher *fsnotify.Watcher) error {
	if !n.cfg.Recursive {
		return watcher.Add(n.cfg.Directory)
	}
	return filepath.WalkDir(n.cfg.Directory, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return watcher.Add(path)
		}
		return nil
	})
}

func (n *node) handleEvent(ctx context.Context, watcher *fsnotify.Watcher, event fsnotify.Event, emit func(string, datagram.Datagram) error) {
	if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
		if n.cfg.Recursive && event.Has(fsnotify.Create) {
			if err := watcher.Add(event.Name); err != nil {
				slog.Warn("file-watch: watching new subdirectory failed", "path", event.Name, "error", err)
			}
		}
		return
	}

	if !event.Has(fsnotify.Create) && !event.Has(fsnotify.Write) {
		return
	}
	matched, err := filepath.Match(n.cfg.Pattern, filepath.Base(event.Name))
	if err != nil || !matched {
		return
	}

	go n.processFile(ctx, event.Name, emit)
}

func (n *node) processFile(ctx context.Context, path string, emit func(string, datagram.Datagram) error) {
	stability := n.cfg.StabilityMs
	if stability == 0 {
		stability = DefaultStabilityMs
	}
	if err := waitStable(ctx, path, stability); err != nil {
		if ctx.Err() == nil {
			slog.Warn("file-watch: waiting for file stability failed", "path", path, "error", err)
		}
		return
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		slog.Warn("file-watch: reading file failed", "path", path, "error", err)
		return
	}

	records, err := parseRecords(n.cfg.Format, raw, n.cfg.CSV, n.cfg.JSONRoot, n.cfg.MalformedRowPolicy)
	if err != nil {
		slog.Warn("file-watch: parsing file failed", "path", path, "error", err)
		return
	}

	values := records
	if n.cfg.Emit == "batch" {
		values = []any{records}
	}
	for _, v := range values {
		d := datagram.New(datagram.Source{NodeID: "file-watch", Origin: path}, datagram.Payload{Value: v})
		if err := emit("out", d); err != nil {
			if ctx.Err() == nil {
				slog.Warn("file-watch: emit failed", "path", path, "error", err)
			}
			return
		}
	}

	if err := n.applyPostAction(path); err != nil {
		slog.Warn("file-watch: post-action failed", "path", path, "error", err)
	}
}

func waitStable(ctx context.Context, path string, stabilityMs int) error {
	var lastSize int64 = -1
	stableSince := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if info.Size() != lastSize {
			lastSize = info.Size()
			stableSince = time.Now()
			continue
		}
		if time.Since(stableSince) >= time.Duration(stabilityMs)*time.Millisecond {
			return nil
		}
	}
}

func (n *node) applyPostAction(path string) error {
	switch n.cfg.PostAction.Action {
	case "", "keep":
		return nil
	case "markerFile":
		return os.WriteFile(path+".processed", nil, 0o644)
	case "move":
		if n.cfg.PostAction.MoveTo == "" {
			return fmt.Errorf("postAction.moveTo is required for action \"move\"")
		}
		return os.Rename(path, filepath.Join(n.cfg.PostAction.MoveTo, filepath.Base(path)))
	case "rename":
		ext := filepath.Ext(path)
		name := strings.TrimSuffix(filepath.Base(path), ext)
		newName := n.cfg.PostAction.RenamePattern
		if newName == "" {
			newName = "{{name}}{{ext}}.done"
		}
		newName = strings.ReplaceAll(newName, "{{name}}", name)
		newName = strings.ReplaceAll(newName, "{{ext}}", ext)
		return os.Rename(path, filepath.Join(filepath.Dir(path), newName))
	case "delete":
		return os.Remove(path)
	default:
		return fmt.Errorf("unknown postAction.action %q", n.cfg.PostAction.Action)
	}
}
