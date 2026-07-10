// Package secsgemaction implements the "secsgem-out" node (SNK-130): sends
// a remote command, a new-equipment-constant update, or an arbitrary raw
// SxFy message to a GEM host connection for each input datagram. Opens its
// own transient HSMS session per datagram (connect, establish
// communications, send, disconnect) rather than holding one open — see
// secsgemshared's package doc for why sharing one persistent session with
// a "secsgem-in" node on the same connection isn't attempted this
// increment.
package secsgemaction

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/gem"
	"github.com/1uedev/DataPipe/engine/nodes/secsgemshared"
	"github.com/1uedev/DataPipe/engine/nodes/secsii"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"action": { "type": "string", "enum": ["remoteCommand", "newEquipmentConstants", "raw"] },
		"rcmd": { "type": "string", "description": "action \"remoteCommand\": the RCMD name (S2F41)." },
		"params": { "type": "object", "additionalProperties": { "type": "string" }, "description": "action \"remoteCommand\": static CPNAME->CPVAL parameters." },
		"paramsFromPayload": { "type": "boolean", "default": false, "description": "action \"remoteCommand\": merge the input payload's own fields (if it's an object) into params, overriding same-named static ones." },
		"equipmentConstants": { "type": "object", "additionalProperties": { "type": "number" }, "description": "action \"newEquipmentConstants\": ECID (as a string key) -> new numeric value (S2F15)." },
		"dataId": { "type": "integer", "default": 0, "description": "action \"newEquipmentConstants\": the DATAID field." },
		"stream": { "type": "integer", "minimum": 0, "maximum": 127, "description": "action \"raw\": the stream number to send." },
		"function": { "type": "integer", "minimum": 0, "maximum": 255, "description": "action \"raw\": the function number to send." },
		"wBit": { "type": "boolean", "default": true, "description": "action \"raw\": whether to wait for and return the reply." }
	},
	"required": ["action"]
}`

func init() {
	flow.Register("secsgem-out", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "SECS/GEM Host Action",
		Category:     flow.CategoryProcessor,
		Description:  "Send a remote command, equipment-constant update, or raw SxFy message to a GEM connection (SNK-130).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "secsgem-out" node's "config" object.
type Config struct {
	Action             string             `json:"action"`
	RCMD               string             `json:"rcmd,omitempty"`
	Params             map[string]string  `json:"params,omitempty"`
	ParamsFromPayload  bool               `json:"paramsFromPayload,omitempty"`
	EquipmentConstants map[string]float64 `json:"equipmentConstants,omitempty"`
	DataID             uint32             `json:"dataId,omitempty"`
	Stream             byte               `json:"stream,omitempty"`
	Function           byte               `json:"function,omitempty"`
	WBit               bool               `json:"wBit,omitempty"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "secsgem-out" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	switch cfg.Action {
	case "remoteCommand":
		if cfg.RCMD == "" {
			return nil, fmt.Errorf("secsgem-out: rcmd is required for action \"remoteCommand\"")
		}
	case "newEquipmentConstants":
		for k := range cfg.EquipmentConstants {
			if _, err := strconv.ParseUint(k, 10, 32); err != nil {
				return nil, fmt.Errorf("secsgem-out: equipmentConstants key %q is not a valid ECID: %w", k, err)
			}
		}
	case "raw":
		// stream/function 0 is technically a valid (if unusual) message; no
		// further validation beyond the schema's enum/range.
	default:
		return nil, fmt.Errorf("secsgem-out: action must be \"remoteCommand\", \"newEquipmentConstants\", or \"raw\", got %q", cfg.Action)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	conn, cfg, err := secsgemshared.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("secsgem-out: %w", err)
	}
	defer func() { _ = conn.Separate() }()

	mdln, softrev := cfg.Identity()
	host := gem.NewHost(conn, mdln, softrev)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = host.Run(runCtx) }()

	commAck, _, _, err := host.EstablishCommunications(ctx)
	if err != nil {
		return nil, fmt.Errorf("secsgem-out: establish communications: %w", err)
	}
	if !commAck.OK() {
		return nil, fmt.Errorf("secsgem-out: establish communications rejected, ack=%d", commAck)
	}

	switch n.cfg.Action {
	case "remoteCommand":
		return n.sendRemoteCommand(ctx, host, in)
	case "newEquipmentConstants":
		return n.sendNewEquipmentConstants(ctx, host, in)
	default:
		return n.sendRaw(ctx, host, in)
	}
}

func (n *node) sendRemoteCommand(ctx context.Context, host *gem.Host, in datagram.Datagram) ([]flow.PortDatagram, error) {
	params := make(map[string]string, len(n.cfg.Params))
	for k, v := range n.cfg.Params {
		params[k] = v
	}
	if n.cfg.ParamsFromPayload {
		for k, v := range secsgemshared.StringMap(in.Payload.Value) {
			params[k] = v
		}
	}
	ack, err := host.SendRemoteCommand(ctx, n.cfg.RCMD, params)
	if err != nil {
		return nil, fmt.Errorf("secsgem-out: remote command %q: %w", n.cfg.RCMD, err)
	}
	if !ack.OK() {
		return nil, fmt.Errorf("secsgem-out: remote command %q rejected, HCACK=%d", n.cfg.RCMD, ack)
	}
	return []flow.PortDatagram{{Port: "out", Datagram: withAckTag(in, "hcack", ack)}}, nil
}

func (n *node) sendNewEquipmentConstants(ctx context.Context, host *gem.Host, in datagram.Datagram) ([]flow.PortDatagram, error) {
	values := make(map[uint32]secsii.Item, len(n.cfg.EquipmentConstants))
	for k, v := range n.cfg.EquipmentConstants {
		ecid, _ := strconv.ParseUint(k, 10, 32) // validated in New
		values[uint32(ecid)] = secsii.F8v(v)
	}
	ack, err := host.SendNewEquipmentConstants(ctx, n.cfg.DataID, values)
	if err != nil {
		return nil, fmt.Errorf("secsgem-out: new equipment constants: %w", err)
	}
	if !ack.OK() {
		return nil, fmt.Errorf("secsgem-out: new equipment constants rejected, EAC=%d", ack)
	}
	return []flow.PortDatagram{{Port: "out", Datagram: withAckTag(in, "eac", ack)}}, nil
}

func (n *node) sendRaw(ctx context.Context, host *gem.Host, in datagram.Datagram) ([]flow.PortDatagram, error) {
	body := secsgemshared.AnyToItem(in.Payload.Value)
	reply, err := host.SendRaw(ctx, n.cfg.Stream, n.cfg.Function, n.cfg.WBit, body)
	if err != nil {
		return nil, fmt.Errorf("secsgem-out: raw S%dF%d: %w", n.cfg.Stream, n.cfg.Function, err)
	}
	out := in
	if n.cfg.WBit {
		out = in.Clone(datagram.DefaultBinaryRefThreshold)
		out.Payload.Value = reply.Value()
	}
	return []flow.PortDatagram{{Port: "out", Datagram: out}}, nil
}

func withAckTag(in datagram.Datagram, key string, ack gem.AckCode) datagram.Datagram {
	out := in.Clone(datagram.DefaultBinaryRefThreshold)
	if out.Header.Tags == nil {
		out.Header.Tags = map[string]string{}
	}
	out.Header.Tags["secsgem."+key] = strconv.Itoa(int(ack))
	return out
}
