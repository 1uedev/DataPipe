package conntest

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/1uedev/DataPipe/engine/nodes/gem"
	"github.com/1uedev/DataPipe/engine/nodes/hsms"
	"github.com/1uedev/DataPipe/engine/nodes/secsgemshared"
)

// SECSGEMBrowseSVIDs performs a live HSMS Select + Establish Communications
// + S1F11 Status Variable Namelist Request against a "secsgem" connection
// (mode "active" only, for the same reason CON-140's own test is
// active-only: the control plane can only dial out, not accept an
// equipment-initiated connection) and returns the equipment's SVID
// catalog — this is MAP-100's report-builder data source for SECS/GEM,
// letting the editor list real SVIDs to pick from instead of requiring the
// user to already know their equipment's identifiers by heart.
func SECSGEMBrowseSVIDs(ctx context.Context, config json.RawMessage) ([]gem.SVIDInfo, error) {
	var cfg secsgemshared.Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if cfg.Mode != "active" {
		return nil, fmt.Errorf("SVID browsing requires a mode \"active\" connection (the equipment must be dialable from the control plane)")
	}
	if cfg.Host == "" || cfg.Port == 0 {
		return nil, fmt.Errorf("host and port are required")
	}

	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	timers := cfg.HSMSTimers()
	timers.T6 = Timeout
	conn, err := hsms.Dial(ctx, cfg.Addr(), uint16(cfg.SessionID), timers)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Separate() }()

	mdln, softrev := cfg.Identity()
	host := gem.NewHost(conn, mdln, softrev)
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	go func() { _ = host.Run(runCtx) }()

	if ack, _, _, err := host.EstablishCommunications(ctx); err != nil {
		return nil, fmt.Errorf("establish communications: %w", err)
	} else if !ack.OK() {
		return nil, fmt.Errorf("establish communications rejected, ack=%d", ack)
	}
	return host.StatusVariableNamelist(ctx)
}
