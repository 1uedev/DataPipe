// Package gemsim is a minimal GEM (SEMI E30) equipment-side simulator used
// to test engine/nodes/gem's Host against something other than a
// hand-scripted single-exchange stand-in: it answers Establish
// Communications, status variable request/namelist, define/link/enable
// report, and establish-trace with real acks, then spontaneously emits
// event reports and trace data on a timer once configured — enough to
// exercise CON-220's "establish communications, event report setup, and
// trace collection" end to end. Not a shipped product feature (SEMI E30's
// equipment-side emulation is P3, see docs/Development-Plan.md); this
// exists purely so the host stack can be tested without real fab hardware,
// per the Development Plan's "test against an equipment simulator from day
// one".
package gemsim

import (
	"context"
	"sync"
	"time"

	"github.com/1uedev/DataPipe/engine/nodes/gem"
	"github.com/1uedev/DataPipe/engine/nodes/hsms"
	"github.com/1uedev/DataPipe/engine/nodes/secsii"
)

// SVID is one simulated status variable / trace source.
type SVID struct {
	Name  string
	Units string
	Value secsii.Item
}

// Config configures a Simulator's fixed identity and status-variable
// catalog.
type Config struct {
	MDLN          string
	SoftRev       string
	SVIDs         map[uint32]SVID
	EventInterval time.Duration // how often an enabled event fires once linked; 0 disables spontaneous events
	TraceInterval time.Duration // how often established trace data is sent; 0 uses the host-requested period parsed as seconds, falling back to 1s if unparseable
}

// Simulator is one simulated equipment, listening passively for one HSMS
// host connection.
type Simulator struct {
	conn *hsms.Conn
	cfg  Config

	mu      sync.Mutex
	reports map[uint32]gem.ReportDef // RPTID -> definition, from S2F33
	links   map[uint32][]uint32      // CEID -> RPTIDs, from S2F35
	traces  map[uint32][]uint32      // TRID -> SVIDs, from S2F23
	cancels []context.CancelFunc     // spontaneous-send goroutines started so far
}

// Listen starts a Simulator listening passively on addr for one HSMS host
// connection (blocks until the host connects and completes Select).
func Listen(ctx context.Context, addr string, cfg Config) (*Simulator, error) {
	conn, err := hsms.Listen(ctx, addr, 0, hsms.DefaultTimers())
	if err != nil {
		return nil, err
	}
	s := &Simulator{
		conn:    conn,
		cfg:     cfg,
		reports: make(map[uint32]gem.ReportDef),
		links:   make(map[uint32][]uint32),
		traces:  make(map[uint32][]uint32),
	}
	go s.serve(ctx)
	return s, nil
}

// Close stops every spontaneous-send goroutine and closes the underlying
// connection.
func (s *Simulator) Close() error {
	s.mu.Lock()
	for _, cancel := range s.cancels {
		cancel()
	}
	s.mu.Unlock()
	return s.conn.Close()
}

func (s *Simulator) serve(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-s.conn.Recv():
			if !ok {
				return
			}
			s.handle(ctx, msg)
		}
	}
}

func (s *Simulator) reply(msg hsms.Message, function byte, body secsii.Item) {
	if !msg.Header.WBit() {
		return
	}
	_ = s.conn.Reply(msg, function, secsii.Encode(body))
}

func (s *Simulator) handle(ctx context.Context, msg hsms.Message) {
	var body secsii.Item
	if len(msg.Body) > 0 {
		body, _, _ = secsii.Decode(msg.Body)
	}
	stream, function := msg.Header.Stream(), msg.Header.Function()

	switch {
	case stream == 1 && function == 1: // Are You There
		s.reply(msg, 2, secsii.L(secsii.A(s.cfg.MDLN), secsii.A(s.cfg.SoftRev)))
	case stream == 1 && function == 13: // Establish Communications
		s.reply(msg, 14, secsii.L(secsii.U1v(0), secsii.L(secsii.A(s.cfg.MDLN), secsii.A(s.cfg.SoftRev))))
	case stream == 1 && function == 3: // Selected Equipment Status Request
		ids := s.svidList(body)
		values := make([]secsii.Item, len(ids))
		for i, id := range ids {
			values[i] = s.cfg.SVIDs[id].Value
		}
		s.reply(msg, 4, secsii.L(values...))
	case stream == 1 && function == 11: // Status Variable Namelist Request
		ids := s.svidList(body)
		entries := make([]secsii.Item, len(ids))
		for i, id := range ids {
			sv := s.cfg.SVIDs[id]
			entries[i] = secsii.L(secsii.U4v(id), secsii.A(sv.Name), secsii.A(sv.Units))
		}
		s.reply(msg, 12, secsii.L(entries...))
	case stream == 2 && function == 17: // Date and Time Request
		s.reply(msg, 18, secsii.A(time.Now().UTC().Format("20060102150405")))
	case stream == 2 && function == 33: // Define Report
		s.handleDefineReport(body)
		s.reply(msg, 34, secsii.L(secsii.U1v(0)))
	case stream == 2 && function == 35: // Link Event Report
		s.handleLinkEventReport(body)
		s.reply(msg, 36, secsii.L(secsii.U1v(0)))
	case stream == 2 && function == 37: // Enable/Disable Event Report
		s.handleEnableEvents(ctx, body)
		s.reply(msg, 38, secsii.L(secsii.U1v(0)))
	case stream == 2 && function == 23: // Establish Trace
		s.handleEstablishTrace(ctx, body)
		s.reply(msg, 24, secsii.L(secsii.U1v(0)))
	case stream == 2 && function == 41: // Host Command Send (remote command)
		s.reply(msg, 42, secsii.L(secsii.U1v(0), secsii.L()))
	case stream == 2 && function == 15: // New Equipment Constant Send
		s.reply(msg, 16, secsii.L(secsii.U1v(0)))
	case stream == 5 && function == 3: // Enable/Disable Alarm
		s.reply(msg, 4, secsii.L(secsii.U1v(0)))
	default:
		s.reply(msg, function+1, secsii.L())
	}
}

// svidList extracts the requested SVID list from an S1F3/S1F11 body,
// returning every configured SVID (in map order) when the request list is
// empty (the "all SVIDs" convention).
func (s *Simulator) svidList(body secsii.Item) []uint32 {
	var ids []uint32
	if body.Format == secsii.FormatList {
		for _, it := range body.List {
			if v, ok := it.Int64(); ok {
				ids = append(ids, uint32(v))
			}
		}
	}
	if len(ids) > 0 {
		return ids
	}
	all := make([]uint32, 0, len(s.cfg.SVIDs))
	for id := range s.cfg.SVIDs {
		all = append(all, id)
	}
	return all
}

func (s *Simulator) handleDefineReport(body secsii.Item) {
	if body.Format != secsii.FormatList || len(body.List) != 2 {
		return
	}
	reportsList := body.List[1]
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range reportsList.List {
		if r.Format != secsii.FormatList || len(r.List) != 2 {
			continue
		}
		rptid, ok := r.List[0].Int64()
		if !ok {
			continue
		}
		var vids []uint32
		for _, v := range r.List[1].List {
			if id, ok := v.Int64(); ok {
				vids = append(vids, uint32(id))
			}
		}
		s.reports[uint32(rptid)] = gem.ReportDef{RPTID: uint32(rptid), VIDs: vids}
	}
}

func (s *Simulator) handleLinkEventReport(body secsii.Item) {
	if body.Format != secsii.FormatList || len(body.List) != 2 {
		return
	}
	linksList := body.List[1]
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range linksList.List {
		if l.Format != secsii.FormatList || len(l.List) != 2 {
			continue
		}
		ceid, ok := l.List[0].Int64()
		if !ok {
			continue
		}
		var rptids []uint32
		for _, r := range l.List[1].List {
			if id, ok := r.Int64(); ok {
				rptids = append(rptids, uint32(id))
			}
		}
		s.links[uint32(ceid)] = rptids
	}
}

func (s *Simulator) handleEnableEvents(ctx context.Context, body secsii.Item) {
	if body.Format != secsii.FormatList || len(body.List) != 2 {
		return
	}
	enabled, _ := body.List[0].Value().(bool)
	if !enabled || s.cfg.EventInterval <= 0 {
		return
	}
	var ceids []uint32
	for _, c := range body.List[1].List {
		if id, ok := c.Int64(); ok {
			ceids = append(ceids, uint32(id))
		}
	}
	if len(ceids) == 0 {
		s.mu.Lock()
		for id := range s.links {
			ceids = append(ceids, id)
		}
		s.mu.Unlock()
	}
	goCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancels = append(s.cancels, cancel)
	s.mu.Unlock()
	go s.fireEventsLoop(goCtx, ceids)
}

func (s *Simulator) fireEventsLoop(ctx context.Context, ceids []uint32) {
	ticker := time.NewTicker(s.cfg.EventInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, ceid := range ceids {
				s.sendEventReport(ctx, ceid)
			}
		}
	}
}

func (s *Simulator) sendEventReport(ctx context.Context, ceid uint32) {
	s.mu.Lock()
	rptids := append([]uint32(nil), s.links[ceid]...)
	reports := make([]secsii.Item, 0, len(rptids))
	for _, rptid := range rptids {
		def := s.reports[rptid]
		values := make([]secsii.Item, len(def.VIDs))
		for i, vid := range def.VIDs {
			values[i] = s.cfg.SVIDs[vid].Value
		}
		reports = append(reports, secsii.L(secsii.U4v(rptid), secsii.L(values...)))
	}
	s.mu.Unlock()

	body := secsii.L(secsii.U4v(1), secsii.U4v(ceid), secsii.L(reports...))
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, _ = s.conn.SendAndWait(sendCtx, 6, 11, secsii.Encode(body))
}

func (s *Simulator) handleEstablishTrace(ctx context.Context, body secsii.Item) {
	if body.Format != secsii.FormatList || len(body.List) != 5 {
		return
	}
	trid, ok := body.List[0].Int64()
	if !ok {
		return
	}
	var svids []uint32
	for _, v := range body.List[4].List {
		if id, ok := v.Int64(); ok {
			svids = append(svids, uint32(id))
		}
	}
	s.mu.Lock()
	s.traces[uint32(trid)] = svids
	s.mu.Unlock()

	if s.cfg.TraceInterval <= 0 {
		return
	}
	goCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancels = append(s.cancels, cancel)
	s.mu.Unlock()
	go s.fireTraceLoop(goCtx, uint32(trid), svids)
}

func (s *Simulator) fireTraceLoop(ctx context.Context, trid uint32, svids []uint32) {
	ticker := time.NewTicker(s.cfg.TraceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			values := make([]secsii.Item, len(svids))
			for i, vid := range svids {
				values[i] = s.cfg.SVIDs[vid].Value
			}
			s.mu.Unlock()

			body := secsii.L(secsii.U4v(trid), secsii.L(values...))
			sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, _ = s.conn.SendAndWait(sendCtx, 6, 1, secsii.Encode(body))
			cancel()
		}
	}
}
