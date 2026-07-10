package gem

import (
	"context"
	"fmt"

	"github.com/1uedev/DataPipe/engine/nodes/hsms"
	"github.com/1uedev/DataPipe/engine/nodes/secsii"
)

// Host is the GEM host role (SEMI E30) on top of one selected hsms.Conn:
// it issues the request/reply message pairs a host initiates, and
// auto-acknowledges the primary messages an equipment initiates (event
// reports, trace data, alarms) while handing their parsed content to the
// caller over channels.
type Host struct {
	conn          *hsms.Conn
	mdln, softrev string
	events        chan EventReport
	traces        chan TraceData
	alarms        chan Alarm
}

// NewHost wraps an already-selected HSMS connection as a GEM host. mdln/
// softrev are this host's own identity, returned to the equipment if it
// ever sends S1F1 (Are You There) to the host.
func NewHost(conn *hsms.Conn, mdln, softrev string) *Host {
	return &Host{
		conn:    conn,
		mdln:    mdln,
		softrev: softrev,
		events:  make(chan EventReport, 16),
		traces:  make(chan TraceData, 64),
		alarms:  make(chan Alarm, 16),
	}
}

// Events returns the channel of parsed S6F11 event reports.
func (h *Host) Events() <-chan EventReport { return h.events }

// Traces returns the channel of parsed S6F1 trace data messages.
func (h *Host) Traces() <-chan TraceData { return h.traces }

// Alarms returns the channel of parsed S5F1 alarm reports.
func (h *Host) Alarms() <-chan Alarm { return h.alarms }

// Run dispatches unsolicited primary messages from the equipment
// (event reports, trace data, alarms, and Are-You-There) until ctx is
// cancelled or the connection fails, auto-acknowledging each per SEMI E30
// before delivering its parsed content to the matching channel. Run must
// be the only reader of conn.Recv() — the node using Host should start it
// in its own goroutine.
func (h *Host) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-h.conn.Err():
			return err
		case msg, ok := <-h.conn.Recv():
			if !ok {
				return nil
			}
			h.handle(msg)
		}
	}
}

func (h *Host) handle(msg hsms.Message) {
	stream, function := msg.Header.Stream(), msg.Header.Function()
	switch {
	case stream == 1 && function == 1: // S1F1 Are You There
		if msg.Header.WBit() {
			_ = h.conn.Reply(msg, 2, secsii.Encode(secsii.L(secsii.A(h.mdln), secsii.A(h.softrev))))
		}
	case stream == 6 && function == 11: // S6F11 Event Report Send
		body, _, err := secsii.Decode(msg.Body)
		if err == nil {
			if ev, err := ParseEventReport(body); err == nil {
				select {
				case h.events <- ev:
				default:
				}
			}
		}
		if msg.Header.WBit() {
			_ = h.conn.Reply(msg, 12, secsii.Encode(EncodeEventReportAck(0)))
		}
	case stream == 6 && function == 1: // S6F1 Trace Data Send
		body, _, err := secsii.Decode(msg.Body)
		if err == nil {
			if td, err := ParseTraceData(body); err == nil {
				select {
				case h.traces <- td:
				default:
				}
			}
		}
		if msg.Header.WBit() {
			_ = h.conn.Reply(msg, 2, secsii.Encode(EncodeTraceDataAck(0)))
		}
	case stream == 5 && function == 1: // S5F1 Alarm Report Send
		body, _, err := secsii.Decode(msg.Body)
		if err == nil {
			if al, err := ParseAlarmReport(body); err == nil {
				select {
				case h.alarms <- al:
				default:
				}
			}
		}
		if msg.Header.WBit() {
			_ = h.conn.Reply(msg, 2, secsii.Encode(EncodeAlarmAck(0)))
		}
	default:
		// Every other unsolicited primary message (raw SxFy the equipment
		// sends unprompted, outside this increment's dictionary subset) is
		// acknowledged with a generic "won't process" reply if it demanded
		// one, so the equipment's own T3 doesn't trip, but otherwise dropped.
		if msg.Header.WBit() {
			_ = h.conn.Reply(msg, msg.Header.Function()+1, secsii.Encode(secsii.L()))
		}
	}
}

func (h *Host) request(ctx context.Context, stream, function byte, body secsii.Item) (secsii.Item, error) {
	reply, err := h.conn.SendAndWait(ctx, stream, function, secsii.Encode(body))
	if err != nil {
		return secsii.Item{}, err
	}
	replyBody, _, err := secsii.Decode(reply.Body)
	if err != nil {
		return secsii.Item{}, fmt.Errorf("gem: decoding S%dF%d reply: %w", stream, function+1, err)
	}
	return replyBody, nil
}

// AreYouThere sends S1F1 and returns the equipment's model/software
// revision from S1F2.
func (h *Host) AreYouThere(ctx context.Context) (mdln, softrev string, err error) {
	reply, err := h.request(ctx, 1, 1, EncodeAreYouThere())
	if err != nil {
		return "", "", err
	}
	return ParseAreYouThereReply(reply)
}

// EstablishCommunications sends S1F13 and returns the equipment's
// acknowledgement and identity from S1F14.
func (h *Host) EstablishCommunications(ctx context.Context) (AckCode, string, string, error) {
	reply, err := h.request(ctx, 1, 13, EncodeEstablishCommunications(h.mdln, h.softrev))
	if err != nil {
		return 0, "", "", err
	}
	return ParseEstablishCommunicationsReply(reply)
}

// RequestStatusVariables sends S1F3 and returns each requested SVID's
// current value (S1F4), in request order.
func (h *Host) RequestStatusVariables(ctx context.Context, svids []uint32) ([]secsii.Item, error) {
	reply, err := h.request(ctx, 1, 3, EncodeStatusVariableRequest(svids))
	if err != nil {
		return nil, err
	}
	return ParseStatusVariableReply(reply)
}

// StatusVariableNamelist sends S1F11 and returns the equipment's SVID
// catalog (S1F12) — the live data behind MAP-100's report builder.
// Passing no svids requests every SVID the equipment has.
func (h *Host) StatusVariableNamelist(ctx context.Context, svids ...uint32) ([]SVIDInfo, error) {
	reply, err := h.request(ctx, 1, 11, EncodeStatusVariableNamelistRequest(svids))
	if err != nil {
		return nil, err
	}
	return ParseStatusVariableNamelistReply(reply)
}

// DateTime sends S2F17 and returns the equipment's clock (S2F18).
func (h *Host) DateTime(ctx context.Context) (string, error) {
	reply, err := h.request(ctx, 2, 17, EncodeDateTimeRequest())
	if err != nil {
		return "", err
	}
	return ParseDateTimeReply(reply)
}

// DefineReport sends S2F33 and returns the equipment's DRACK (S2F34).
func (h *Host) DefineReport(ctx context.Context, dataID uint32, reports []ReportDef) (AckCode, error) {
	reply, err := h.request(ctx, 2, 33, EncodeDefineReport(dataID, reports))
	if err != nil {
		return 0, err
	}
	return ParseDefineReportReply(reply)
}

// LinkEventReport sends S2F35 and returns the equipment's LRACK (S2F36).
func (h *Host) LinkEventReport(ctx context.Context, dataID uint32, links []EventLink) (AckCode, error) {
	reply, err := h.request(ctx, 2, 35, EncodeLinkEventReport(dataID, links))
	if err != nil {
		return 0, err
	}
	return ParseLinkEventReportReply(reply)
}

// EnableEvents sends S2F37 and returns the equipment's ERACK (S2F38).
func (h *Host) EnableEvents(ctx context.Context, enable bool, ceids []uint32) (AckCode, error) {
	reply, err := h.request(ctx, 2, 37, EncodeEnableEvents(enable, ceids))
	if err != nil {
		return 0, err
	}
	return ParseEnableEventsReply(reply)
}

// EstablishTrace sends S2F23 (trace initialize) and returns the
// equipment's TIAACK (S2F24). Subsequent S6F1 trace data arrives on
// Traces() and is auto-acknowledged by Run.
func (h *Host) EstablishTrace(ctx context.Context, trid uint32, period string, totalSamples, reportEvery uint32, svids []uint32) (AckCode, error) {
	reply, err := h.request(ctx, 2, 23, EncodeEstablishTrace(trid, period, totalSamples, reportEvery, svids))
	if err != nil {
		return 0, err
	}
	return ParseEstablishTraceReply(reply)
}

// EnableAlarm sends S2F(5F)3 and returns the equipment's ACKC5 (S5F4).
func (h *Host) EnableAlarm(ctx context.Context, enable bool, alid uint32) (AckCode, error) {
	reply, err := h.request(ctx, 5, 3, EncodeEnableAlarm(enable, alid))
	if err != nil {
		return 0, err
	}
	return ParseEnableAlarmReply(reply)
}

// SendRemoteCommand sends S2F41 (SNK-130 host action) and returns the
// equipment's HCACK (S2F42).
func (h *Host) SendRemoteCommand(ctx context.Context, rcmd string, params map[string]string) (AckCode, error) {
	reply, err := h.request(ctx, 2, 41, EncodeRemoteCommand(rcmd, params))
	if err != nil {
		return 0, err
	}
	return ParseRemoteCommandReply(reply)
}

// SendNewEquipmentConstants sends S2F15 (SNK-130 ECID update) and returns
// the equipment's EAC (S2F16).
func (h *Host) SendNewEquipmentConstants(ctx context.Context, dataID uint32, values map[uint32]secsii.Item) (AckCode, error) {
	reply, err := h.request(ctx, 2, 15, EncodeNewEquipmentConstants(dataID, values))
	if err != nil {
		return 0, err
	}
	return ParseNewEquipmentConstantsReply(reply)
}

// SendRaw sends an arbitrary SxFy message (SNK-130's "raw SxFy access")
// and, if wBit is set, waits for and returns the reply body; otherwise it
// returns immediately with a zero-value Item.
func (h *Host) SendRaw(ctx context.Context, stream, function byte, wBit bool, body secsii.Item) (secsii.Item, error) {
	if !wBit {
		return secsii.Item{}, h.conn.Send(stream, function, secsii.Encode(body))
	}
	return h.request(ctx, stream, function, body)
}
