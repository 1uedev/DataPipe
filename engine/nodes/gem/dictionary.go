// Package gem implements the GEM (SEMI E30) message dictionary and a
// host-role state machine on top of HSMS (engine/nodes/hsms) and SECS-II
// (engine/nodes/secsii): establish communications, status variable and
// equipment-constant access, collection-event report definition/linking/
// enabling, trace data collection, alarm management, and remote commands.
// Scope for this increment covers the streams/functions needed for CON-220's
// "establish communications, event report setup, and trace collection" —
// recipe management and SECS-I are out of scope (P2, see TODO.md).
package gem

import (
	"fmt"

	"github.com/1uedev/DataPipe/engine/nodes/secsii"
)

// AckCode is the single-byte accept/reject code shared by DRACK, LRACK,
// ERACK, TIAACK, ACKC5, ACKC6, EAC, and HCACK (SEMI E30: 0 always means
// "accepted"; nonzero reasons are message-specific and surfaced as-is).
type AckCode byte

// OK reports whether the code means "accepted" (always 0 across every GEM
// ack code this package uses).
func (a AckCode) OK() bool { return a == 0 }

// ---- S1F1/F2: Are You There ----

// EncodeAreYouThere returns S1F1's (empty) body.
func EncodeAreYouThere() secsii.Item { return secsii.L() }

// ParseAreYouThereReply parses S1F2's body: equipment model + software
// revision. An empty list (the "not communicating" convention) yields
// empty strings.
func ParseAreYouThereReply(body secsii.Item) (mdln, softrev string, err error) {
	if body.Format != secsii.FormatList {
		return "", "", fmt.Errorf("gem: S1F2 body must be a list")
	}
	if len(body.List) == 0 {
		return "", "", nil
	}
	if len(body.List) != 2 {
		return "", "", fmt.Errorf("gem: S1F2 body must have 0 or 2 elements, got %d", len(body.List))
	}
	mdln, _ = body.List[0].Text()
	softrev, _ = body.List[1].Text()
	return mdln, softrev, nil
}

// ---- S1F13/F14: Establish Communications ----

// EncodeEstablishCommunications returns S1F13's body: this host's own
// model/software-revision identity.
func EncodeEstablishCommunications(mdln, softrev string) secsii.Item {
	return secsii.L(secsii.A(mdln), secsii.A(softrev))
}

// ParseEstablishCommunicationsReply parses S1F14's body: L(COMMACK,
// L(MDLN, SOFTREV)).
func ParseEstablishCommunicationsReply(body secsii.Item) (commack AckCode, mdln, softrev string, err error) {
	if body.Format != secsii.FormatList || len(body.List) != 2 {
		return 0, "", "", fmt.Errorf("gem: S1F14 body must be a 2-element list")
	}
	ack, ok := body.List[0].Int64()
	if !ok {
		return 0, "", "", fmt.Errorf("gem: S1F14 COMMACK must be numeric")
	}
	inner := body.List[1]
	if inner.Format != secsii.FormatList {
		return AckCode(ack), "", "", nil
	}
	if len(inner.List) == 2 {
		mdln, _ = inner.List[0].Text()
		softrev, _ = inner.List[1].Text()
	}
	return AckCode(ack), mdln, softrev, nil
}

// ---- S1F3/F4: Selected Equipment Status ----

// EncodeStatusVariableRequest returns S1F3's body: the list of SVIDs to
// read (empty means "all").
func EncodeStatusVariableRequest(svids []uint32) secsii.Item {
	items := make([]secsii.Item, len(svids))
	for i, v := range svids {
		items[i] = secsii.U4v(v)
	}
	return secsii.L(items...)
}

// ParseStatusVariableReply parses S1F4's body: one item per requested
// SVID, in the equipment's own chosen format, in request order.
func ParseStatusVariableReply(body secsii.Item) ([]secsii.Item, error) {
	if body.Format != secsii.FormatList {
		return nil, fmt.Errorf("gem: S1F4 body must be a list")
	}
	return body.List, nil
}

// ---- S1F11/F12: Status Variable Namelist ----

// SVIDInfo is one entry of S1F12's reply.
type SVIDInfo struct {
	SVID  uint32
	Name  string
	Units string
}

// EncodeStatusVariableNamelistRequest returns S1F11's body (empty = all
// SVIDs the equipment has).
func EncodeStatusVariableNamelistRequest(svids []uint32) secsii.Item {
	return EncodeStatusVariableRequest(svids)
}

// ParseStatusVariableNamelistReply parses S1F12's body: a list of
// L(SVID, SVNAM, UNITS) triples.
func ParseStatusVariableNamelistReply(body secsii.Item) ([]SVIDInfo, error) {
	if body.Format != secsii.FormatList {
		return nil, fmt.Errorf("gem: S1F12 body must be a list")
	}
	out := make([]SVIDInfo, 0, len(body.List))
	for i, entry := range body.List {
		if entry.Format != secsii.FormatList || len(entry.List) != 3 {
			return nil, fmt.Errorf("gem: S1F12 entry %d must be a 3-element list", i)
		}
		svid, ok := entry.List[0].Int64()
		if !ok {
			return nil, fmt.Errorf("gem: S1F12 entry %d: SVID must be numeric", i)
		}
		name, _ := entry.List[1].Text()
		units, _ := entry.List[2].Text()
		out = append(out, SVIDInfo{SVID: uint32(svid), Name: name, Units: units})
	}
	return out, nil
}

// ---- S2F17/F18: Date and Time ----

// EncodeDateTimeRequest returns S2F17's (empty) body.
func EncodeDateTimeRequest() secsii.Item { return secsii.L() }

// ParseDateTimeReply parses S2F18's body: a single ASCII timestamp.
func ParseDateTimeReply(body secsii.Item) (string, error) {
	s, ok := body.Text()
	if !ok {
		return "", fmt.Errorf("gem: S2F18 body must be ASCII")
	}
	return s, nil
}

// ---- S2F33/F34: Define Report ----

// ReportDef is one report definition: an RPTID and the VIDs it collects.
type ReportDef struct {
	RPTID uint32
	VIDs  []uint32
}

// EncodeDefineReport returns S2F33's body: L(DATAID, L(report...)) where
// each report is L(RPTID, L(VID...)). An empty reports slice deletes all
// existing report definitions (SEMI E30 convention).
func EncodeDefineReport(dataID uint32, reports []ReportDef) secsii.Item {
	reportItems := make([]secsii.Item, len(reports))
	for i, r := range reports {
		vids := make([]secsii.Item, len(r.VIDs))
		for j, v := range r.VIDs {
			vids[j] = secsii.U4v(v)
		}
		reportItems[i] = secsii.L(secsii.U4v(r.RPTID), secsii.L(vids...))
	}
	return secsii.L(secsii.U4v(dataID), secsii.L(reportItems...))
}

// ParseDefineReportReply parses S2F34's body: L(DRACK).
func ParseDefineReportReply(body secsii.Item) (AckCode, error) {
	return parseSingleAck(body, "S2F34", "DRACK")
}

// ---- S2F35/F36: Link Event Report ----

// EventLink links a CEID to the RPTIDs it should emit when it fires.
type EventLink struct {
	CEID   uint32
	RPTIDs []uint32
}

// EncodeLinkEventReport returns S2F35's body: L(DATAID, L(link...)) where
// each link is L(CEID, L(RPTID...)).
func EncodeLinkEventReport(dataID uint32, links []EventLink) secsii.Item {
	linkItems := make([]secsii.Item, len(links))
	for i, l := range links {
		rptids := make([]secsii.Item, len(l.RPTIDs))
		for j, r := range l.RPTIDs {
			rptids[j] = secsii.U4v(r)
		}
		linkItems[i] = secsii.L(secsii.U4v(l.CEID), secsii.L(rptids...))
	}
	return secsii.L(secsii.U4v(dataID), secsii.L(linkItems...))
}

// ParseLinkEventReportReply parses S2F36's body: L(LRACK).
func ParseLinkEventReportReply(body secsii.Item) (AckCode, error) {
	return parseSingleAck(body, "S2F36", "LRACK")
}

// ---- S2F37/F38: Enable/Disable Event Report ----

// EncodeEnableEvents returns S2F37's body: L(CEED, L(CEID...)). An empty
// ceids slice means "every collection event".
func EncodeEnableEvents(enable bool, ceids []uint32) secsii.Item {
	items := make([]secsii.Item, len(ceids))
	for i, c := range ceids {
		items[i] = secsii.U4v(c)
	}
	return secsii.L(secsii.Bool(enable), secsii.L(items...))
}

// ParseEnableEventsReply parses S2F38's body: L(ERACK).
func ParseEnableEventsReply(body secsii.Item) (AckCode, error) {
	return parseSingleAck(body, "S2F38", "ERACK")
}

// ---- S2F23/F24: Establish (Trace) ----

// EncodeEstablishTrace returns S2F23's body: L(TRID, DSPER, TOTSMP, REPGSZ,
// L(SVID...)). DSPER is the sampling period, SEMI E30's "SSSS.mmm" seconds-
// dot-milliseconds text form. totalSamples 0 means "until DisableTrace".
func EncodeEstablishTrace(trid uint32, period string, totalSamples, reportEvery uint32, svids []uint32) secsii.Item {
	items := make([]secsii.Item, len(svids))
	for i, v := range svids {
		items[i] = secsii.U4v(v)
	}
	return secsii.L(secsii.U4v(trid), secsii.A(period), secsii.U4v(totalSamples), secsii.U4v(reportEvery), secsii.L(items...))
}

// ParseEstablishTraceReply parses S2F24's body: L(TIAACK).
func ParseEstablishTraceReply(body secsii.Item) (AckCode, error) {
	return parseSingleAck(body, "S2F24", "TIAACK")
}

// ---- S6F1/F2: Trace Data ----

// TraceData is one S6F1 trace-data message: the TRID it belongs to and the
// SVID values sampled in the configured order.
type TraceData struct {
	TRID   uint32
	Values []secsii.Item
}

// ParseTraceData parses S6F1's body: L(TRID, L(value...)).
func ParseTraceData(body secsii.Item) (TraceData, error) {
	if body.Format != secsii.FormatList || len(body.List) != 2 {
		return TraceData{}, fmt.Errorf("gem: S6F1 body must be a 2-element list")
	}
	trid, ok := body.List[0].Int64()
	if !ok {
		return TraceData{}, fmt.Errorf("gem: S6F1 TRID must be numeric")
	}
	values := body.List[1]
	if values.Format != secsii.FormatList {
		return TraceData{}, fmt.Errorf("gem: S6F1 values must be a list")
	}
	return TraceData{TRID: uint32(trid), Values: values.List}, nil
}

// EncodeTraceDataAck returns S6F2's body: L(ACKC6).
func EncodeTraceDataAck(ack AckCode) secsii.Item {
	return secsii.L(secsii.U1v(byte(ack)))
}

// ---- S6F11/F12: Event Report Send ----

// ReportData is one collected report inside an event report send.
type ReportData struct {
	RPTID  uint32
	Values []secsii.Item
}

// EventReport is one S6F11 event report send.
type EventReport struct {
	DataID  uint32
	CEID    uint32
	Reports []ReportData
}

// ParseEventReport parses S6F11's body: L(DATAID, CEID, L(report...)) where
// each report is L(RPTID, L(value...)).
func ParseEventReport(body secsii.Item) (EventReport, error) {
	if body.Format != secsii.FormatList || len(body.List) != 3 {
		return EventReport{}, fmt.Errorf("gem: S6F11 body must be a 3-element list")
	}
	dataID, ok := body.List[0].Int64()
	if !ok {
		return EventReport{}, fmt.Errorf("gem: S6F11 DATAID must be numeric")
	}
	ceid, ok := body.List[1].Int64()
	if !ok {
		return EventReport{}, fmt.Errorf("gem: S6F11 CEID must be numeric")
	}
	reportsList := body.List[2]
	if reportsList.Format != secsii.FormatList {
		return EventReport{}, fmt.Errorf("gem: S6F11 reports must be a list")
	}
	reports := make([]ReportData, 0, len(reportsList.List))
	for i, r := range reportsList.List {
		if r.Format != secsii.FormatList || len(r.List) != 2 {
			return EventReport{}, fmt.Errorf("gem: S6F11 report %d must be a 2-element list", i)
		}
		rptid, ok := r.List[0].Int64()
		if !ok {
			return EventReport{}, fmt.Errorf("gem: S6F11 report %d: RPTID must be numeric", i)
		}
		values := r.List[1]
		if values.Format != secsii.FormatList {
			return EventReport{}, fmt.Errorf("gem: S6F11 report %d: values must be a list", i)
		}
		reports = append(reports, ReportData{RPTID: uint32(rptid), Values: values.List})
	}
	return EventReport{DataID: uint32(dataID), CEID: uint32(ceid), Reports: reports}, nil
}

// EncodeEventReportAck returns S6F12's body: L(ACKC6).
func EncodeEventReportAck(ack AckCode) secsii.Item {
	return secsii.L(secsii.U1v(byte(ack)))
}

// ---- S5F1/F2: Alarm Report Send ----

// Alarm is one S5F1 alarm report.
type Alarm struct {
	Set  bool // ALCD's high bit: true = alarm set, false = alarm cleared
	Code byte // ALCD's low 7 bits: alarm category
	ALID uint32
	Text string
}

// ParseAlarmReport parses S5F1's body: L(ALCD, ALID, ALTX).
func ParseAlarmReport(body secsii.Item) (Alarm, error) {
	if body.Format != secsii.FormatList || len(body.List) != 3 {
		return Alarm{}, fmt.Errorf("gem: S5F1 body must be a 3-element list")
	}
	alcd, ok := body.List[0].Int64()
	if !ok {
		return Alarm{}, fmt.Errorf("gem: S5F1 ALCD must be numeric")
	}
	alid, ok := body.List[1].Int64()
	if !ok {
		return Alarm{}, fmt.Errorf("gem: S5F1 ALID must be numeric")
	}
	text, _ := body.List[2].Text()
	return Alarm{Set: alcd&0x80 != 0, Code: byte(alcd) & 0x7F, ALID: uint32(alid), Text: text}, nil
}

// EncodeAlarmAck returns S5F2's body: L(ACKC5).
func EncodeAlarmAck(ack AckCode) secsii.Item {
	return secsii.L(secsii.U1v(byte(ack)))
}

// EncodeEnableAlarm returns S5F3's body: L(ALED, ALID).
func EncodeEnableAlarm(enable bool, alid uint32) secsii.Item {
	return secsii.L(secsii.Bool(enable), secsii.U4v(alid))
}

// ParseEnableAlarmReply parses S5F4's body: L(ACKC5).
func ParseEnableAlarmReply(body secsii.Item) (AckCode, error) {
	return parseSingleAck(body, "S5F4", "ACKC5")
}

// ---- S2F41/F42: Host Command Send (Remote Command) ----

// EncodeRemoteCommand returns S2F41's body: L(RCMD, L(L(CPNAME, CPVAL)...)).
func EncodeRemoteCommand(rcmd string, params map[string]string) secsii.Item {
	paramItems := make([]secsii.Item, 0, len(params))
	for name, val := range params {
		paramItems = append(paramItems, secsii.L(secsii.A(name), secsii.A(val)))
	}
	return secsii.L(secsii.A(rcmd), secsii.L(paramItems...))
}

// ParseRemoteCommandReply parses S2F42's body: L(HCACK, L(...)). Only the
// overall HCACK is surfaced — per-parameter CPACKs are not (a documented
// simplification; see TODO.md).
func ParseRemoteCommandReply(body secsii.Item) (AckCode, error) {
	if body.Format != secsii.FormatList || len(body.List) < 1 {
		return 0, fmt.Errorf("gem: S2F42 body must be a non-empty list")
	}
	hcack, ok := body.List[0].Int64()
	if !ok {
		return 0, fmt.Errorf("gem: S2F42 HCACK must be numeric")
	}
	return AckCode(hcack), nil
}

// ---- S2F15/F16: New Equipment Constant ----

// EncodeNewEquipmentConstants returns S2F15's body: L(DATAID,
// L(L(ECID, ECV)...)).
func EncodeNewEquipmentConstants(dataID uint32, values map[uint32]secsii.Item) secsii.Item {
	items := make([]secsii.Item, 0, len(values))
	for ecid, v := range values {
		items = append(items, secsii.L(secsii.U4v(ecid), v))
	}
	return secsii.L(secsii.U4v(dataID), secsii.L(items...))
}

// ParseNewEquipmentConstantsReply parses S2F16's body: L(EAC).
func ParseNewEquipmentConstantsReply(body secsii.Item) (AckCode, error) {
	return parseSingleAck(body, "S2F16", "EAC")
}

func parseSingleAck(body secsii.Item, msg, field string) (AckCode, error) {
	if body.Format != secsii.FormatList || len(body.List) != 1 {
		return 0, fmt.Errorf("gem: %s body must be a 1-element list", msg)
	}
	v, ok := body.List[0].Int64()
	if !ok {
		return 0, fmt.Errorf("gem: %s %s must be numeric", msg, field)
	}
	return AckCode(v), nil
}
