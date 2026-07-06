# TODO — Next Steps

Working queue for DataPipe. Top item is always next. Detail and acceptance criteria live in `docs/Development-Plan.md`; this file only tracks order and status. When a step is finished, move its entry to `DONE.md` with date and commit hash.

## Now

- [ ] **Increment 5 — Live debugging** (inspector, debug sidebar, wire animation, data pinning)

## Next (in order, from docs/Development-Plan.md)

- [ ] Increment 6 — First real connectors (MQTT, HTTP/REST, schedule, files, Postgres, bus topics)
- [ ] Increment 7 — Processor library P1 (script sandbox, window/aggregate, switch, expressions)
- [ ] Increment 8 — Triggered workflows (execution history, error flows, DLQ, re-run)
- [ ] Increment 9 — Edge runtime + fleet (enrollment, store-and-forward, runtime groups)
- [ ] Increment 10 — Remaining P1 connectors + hardening (OPC-UA, Modbus, Kafka, soak test)
- [ ] Increment 11 — SECS/GEM track (HSMS spike → codec → GEM host; parallel from Inc. 6)

## Backlog / unscheduled

- [ ] **Standing item**: update `docs/User-Guide.md` and `docs/Admin-Guide.md` at the end of every increment (they document the state after Increment 4; NFR-310/320 require them complete and offline-available by 1.0)

- [ ] Equipment simulator selection for SECS/GEM testing (Increment 11 prerequisite)
- [ ] Usability test participants for NFR-300 (first-flow-in-15-minutes criterion)
- [ ] Runtime↔control-plane gRPC channel currently dials with insecure credentials (walking-skeleton placeholder) — add TLS per Architecture §2.5/ADR-007 before edge runtimes (Increment 9) connect over untrusted networks
- [ ] `engine/datagram.Datagram.Clone` deep-copies Tags and small binary payloads but not generic map/slice `Payload.Value` — a node that mutates a map payload in place could leak the mutation across fan-out branches sharing that map; the `set` node works around this locally (its own deep-copy) but the general `Clone` gap remains (DGM-120/BUS-140)
- [ ] Flow-File-Format §7 rule 3 (node-config JSON-Schema validation) still not implemented — `engine/flow.Validate` covers ids/wires/mode/registered-types only; rule 2 (connection-ref resolution) is now moot for control-plane-issued deploys since flows there reference control-plane connection ids, but full validation still needs the node-manifest schema system (SDK track)
- [ ] ENG-150 resource guardrails only partially covered so far (bounded queues via `bus.Wire` capacity/overflow, panic recovery via ARC-150); script CPU/time/memory limits need the goja sandbox (Increment 7)
- [ ] ERR-120 (flow-level error handler) and ERR-130 (dead-letter topic) are not implemented — only the per-node ERR-100 policy (fail/retry/errorPort/discard) exists
- [ ] `registry.Service.DeployFlow` broadcasts every deploy to every connected runtime — Flow-File-Format's `runtimeAssignment`/UI-220 (deploy to a specific runtime/edge group) isn't implemented; needs real fleet/group targeting in Increment 9
- [ ] SEC-100 P2 items deferred: TOTP 2FA, OIDC/SAML SSO, LDAP/AD sync; SEC-110 custom granular roles (P2); SEC-120 external vault integration and a KEK-rotation admin operation (the versioned envelope mechanism that enables rotation exists, but nothing yet re-wraps existing DEKs under a new KEK)
- [ ] API-110 (WebSocket/live channel for debug/deploy-status/runtime-health) not implemented — lands with live debugging (Increment 5)
- [ ] API-120 versioning/deprecation policy and scoped API keys (as opposed to session tokens) not implemented
- [ ] VCS-120 (git integration) and VCS-150 (deployment pipeline promotion with approval gates) are P2, deferred
- [ ] Increment 4 (editor) scope explicitly excluded per Development-Plan's "subflow-less editing": UI-140 (subflows). Also not implemented (not listed in the Increment 4 bullet, deferred to their own later work): UI-180 auto-layout/printable docs (P2), UI-220 deploy-target/runtime-group selection (Increment 9 fleet work), UI-230 concurrent-editing presence (P2), UI-320 accessibility audit (P2), UI-330 onboarding tutorial + template gallery, UI-150 visual groups/sticky notes, UI-130 quick-insert-on-wire and live throughput labels, UI-120 live datagrams/sec + last-value indicators (all need DBG-170 live data, Increment 5)
- [ ] UI-200's "scope choice: full / modified flows only / modified nodes only" isn't a UI toggle — the editor always deploys one flow at a time (matching the REST API's per-flow deploy endpoint); modified-nodes-only behavior already happens automatically server-side via ENG-140 hot deploy regardless of what the UI offers
- [ ] Development-Plan Increment 4's "usability check with one target-persona user" is a human deliverable — not something an agent can perform; golden path (build + deploy inject→set→debug-log in the browser) was verified thoroughly via manual browser testing instead, but the actual usability test with a real target-persona user is still outstanding
- [ ] Automated browser verification of drawing a NEW wire by dragging (React Flow's pointer-capture-based connection gesture) didn't work via synthetic PointerEvents in headless testing — a testing-tool limitation, not a demonstrated app defect; the underlying `onConnect`/`reconnectEdge` store logic is unit-tested, and loading/saving/deploying a flow with pre-existing wires was verified live end to end. Worth a follow-up manual check in a real (non-headless) browser
