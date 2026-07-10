// Package secsgemhost implements the "secsgem-in" node (CON-220): a GEM
// host connection that establishes communications, applies configured
// report/event/trace setup at startup, and emits one datagram per received
// event report, trace-data message, or alarm on its "events"/"traces"/
// "alarms" ports respectively.
package secsgemhost

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/gem"
	"github.com/1uedev/DataPipe/engine/nodes/secsgemshared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"reports": {
			"type": "array",
			"description": "S2F33 Define Report: report definitions this host owns.",
			"items": {
				"type": "object",
				"properties": {
					"rptid": { "type": "integer" },
					"vids": { "type": "array", "items": { "type": "integer" } }
				},
				"required": ["rptid", "vids"]
			}
		},
		"events": {
			"type": "array",
			"description": "S2F35 Link Event Report + S2F37 Enable Event Report: which reports fire on which collection events, enabled at startup.",
			"items": {
				"type": "object",
				"properties": {
					"ceid": { "type": "integer" },
					"rptids": { "type": "array", "items": { "type": "integer" } }
				},
				"required": ["ceid", "rptids"]
			}
		},
		"traces": {
			"type": "array",
			"description": "S2F23 Establish Trace, one entry per trace request.",
			"items": {
				"type": "object",
				"properties": {
					"trid": { "type": "integer" },
					"periodSec": { "type": "number", "description": "Sampling period in seconds (fractional allowed, e.g. 0.5)." },
					"totalSamples": { "type": "integer", "default": 0, "description": "0 = continuous until the connection closes." },
					"reportEvery": { "type": "integer", "default": 1 },
					"svids": { "type": "array", "items": { "type": "integer" } }
				},
				"required": ["trid", "periodSec", "svids"]
			}
		}
	}
}`

func init() {
	flow.Register("secsgem-in", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"events", "traces", "alarms"},
		DisplayName:  "SECS/GEM Host",
		Category:     flow.CategorySource,
		Description:  "GEM host: establishes communications, sets up event report/trace collection, emits event reports/trace data/alarms (CON-220).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

type reportCfg struct {
	RPTID uint32   `json:"rptid"`
	VIDs  []uint32 `json:"vids"`
}

type eventCfg struct {
	CEID   uint32   `json:"ceid"`
	RPTIDs []uint32 `json:"rptids"`
}

type traceCfg struct {
	TRID         uint32   `json:"trid"`
	PeriodSec    float64  `json:"periodSec"`
	TotalSamples uint32   `json:"totalSamples,omitempty"`
	ReportEvery  uint32   `json:"reportEvery,omitempty"`
	SVIDs        []uint32 `json:"svids"`
}

// Config is the "secsgem-in" node's "config" object.
type Config struct {
	Reports []reportCfg `json:"reports,omitempty"`
	Events  []eventCfg  `json:"events,omitempty"`
	Traces  []traceCfg  `json:"traces,omitempty"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "secsgem-in" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	for i, r := range cfg.Reports {
		if len(r.VIDs) == 0 {
			return nil, fmt.Errorf("secsgem-in: reports[%d]: vids is required", i)
		}
	}
	for i, e := range cfg.Events {
		if len(e.RPTIDs) == 0 {
			return nil, fmt.Errorf("secsgem-in: events[%d]: rptids is required", i)
		}
	}
	for i, tr := range cfg.Traces {
		if len(tr.SVIDs) == 0 {
			return nil, fmt.Errorf("secsgem-in: traces[%d]: svids is required", i)
		}
		if tr.PeriodSec <= 0 {
			return nil, fmt.Errorf("secsgem-in: traces[%d]: periodSec must be positive", i)
		}
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	conn, cfg, err := secsgemshared.Connect(ctx)
	if err != nil {
		return fmt.Errorf("secsgem-in: %w", err)
	}
	defer func() { _ = conn.Close() }()

	mdln, softrev := cfg.Identity()
	host := gem.NewHost(conn, mdln, softrev)

	runErrCh := make(chan error, 1)
	go func() { runErrCh <- host.Run(ctx) }()

	if err := n.setup(ctx, host); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-runErrCh:
			return fmt.Errorf("secsgem-in: connection: %w", err)
		case ev := <-host.Events():
			if err := emit("events", eventDatagram(ev)); err != nil {
				return err
			}
		case td := <-host.Traces():
			if err := emit("traces", traceDatagram(td)); err != nil {
				return err
			}
		case al := <-host.Alarms():
			if err := emit("alarms", alarmDatagram(al)); err != nil {
				return err
			}
		}
	}
}

func (n *node) setup(ctx context.Context, host *gem.Host) error {
	ack, _, _, err := host.EstablishCommunications(ctx)
	if err != nil {
		return fmt.Errorf("secsgem-in: establish communications: %w", err)
	}
	if !ack.OK() {
		return fmt.Errorf("secsgem-in: establish communications rejected, ack=%d", ack)
	}

	if len(n.cfg.Reports) > 0 {
		reports := make([]gem.ReportDef, len(n.cfg.Reports))
		for i, r := range n.cfg.Reports {
			reports[i] = gem.ReportDef{RPTID: r.RPTID, VIDs: r.VIDs}
		}
		ack, err := host.DefineReport(ctx, 1, reports)
		if err != nil {
			return fmt.Errorf("secsgem-in: define report: %w", err)
		}
		if !ack.OK() {
			return fmt.Errorf("secsgem-in: define report rejected, ack=%d", ack)
		}
	}

	if len(n.cfg.Events) > 0 {
		links := make([]gem.EventLink, len(n.cfg.Events))
		ceids := make([]uint32, len(n.cfg.Events))
		for i, e := range n.cfg.Events {
			links[i] = gem.EventLink{CEID: e.CEID, RPTIDs: e.RPTIDs}
			ceids[i] = e.CEID
		}
		ack, err := host.LinkEventReport(ctx, 2, links)
		if err != nil {
			return fmt.Errorf("secsgem-in: link event report: %w", err)
		}
		if !ack.OK() {
			return fmt.Errorf("secsgem-in: link event report rejected, ack=%d", ack)
		}
		enableAck, err := host.EnableEvents(ctx, true, ceids)
		if err != nil {
			return fmt.Errorf("secsgem-in: enable event report: %w", err)
		}
		if !enableAck.OK() {
			return fmt.Errorf("secsgem-in: enable event report rejected, ack=%d", enableAck)
		}
	}

	for _, tr := range n.cfg.Traces {
		reportEvery := tr.ReportEvery
		if reportEvery == 0 {
			reportEvery = 1
		}
		period := formatDSPER(tr.PeriodSec)
		ack, err := host.EstablishTrace(ctx, tr.TRID, period, tr.TotalSamples, reportEvery, tr.SVIDs)
		if err != nil {
			return fmt.Errorf("secsgem-in: establish trace %d: %w", tr.TRID, err)
		}
		if !ack.OK() {
			return fmt.Errorf("secsgem-in: establish trace %d rejected, ack=%d", tr.TRID, ack)
		}
	}
	return nil
}

// formatDSPER renders a sampling period as SEMI E30's "SSSS.mmm"
// seconds-dot-milliseconds text form.
func formatDSPER(periodSec float64) string {
	seconds := int(periodSec)
	millis := int((periodSec - float64(seconds)) * 1000)
	return fmt.Sprintf("%04d.%03d", seconds, millis)
}

func eventDatagram(ev gem.EventReport) datagram.Datagram {
	reports := make(map[string]any, len(ev.Reports))
	for _, r := range ev.Reports {
		values := make([]any, len(r.Values))
		for i, v := range r.Values {
			values[i] = v.Value()
		}
		reports[fmt.Sprintf("%d", r.RPTID)] = values
	}
	d := datagram.New(datagram.Source{NodeID: "secsgem-in"}, datagram.Payload{Value: map[string]any{
		"dataId":  ev.DataID,
		"ceid":    ev.CEID,
		"reports": reports,
	}})
	return d
}

func traceDatagram(td gem.TraceData) datagram.Datagram {
	values := make([]any, len(td.Values))
	for i, v := range td.Values {
		values[i] = v.Value()
	}
	return datagram.New(datagram.Source{NodeID: "secsgem-in"}, datagram.Payload{Value: map[string]any{
		"trid":   td.TRID,
		"values": values,
	}})
}

func alarmDatagram(al gem.Alarm) datagram.Datagram {
	return datagram.New(datagram.Source{NodeID: "secsgem-in"}, datagram.Payload{Value: map[string]any{
		"set":  al.Set,
		"code": al.Code,
		"alid": al.ALID,
		"text": al.Text,
	}})
}
