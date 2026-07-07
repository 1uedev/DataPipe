# TODO — Next Steps

Working queue for DataPipe. Top item is always next. Detail and acceptance criteria live in `docs/Development-Plan.md`; this file only tracks order and status. When a step is finished, move its entry to `DONE.md` with date and commit hash.

## Now

- [ ] **Increment 7 — Processor library P1** (script sandbox, window/aggregate, switch, expressions)

## Next (in order, from docs/Development-Plan.md)

- [ ] Increment 8 — Triggered workflows (execution history, error flows, DLQ, re-run)
- [ ] Increment 9 — Edge runtime + fleet (enrollment, store-and-forward, runtime groups)
- [ ] Increment 10 — Remaining P1 connectors + hardening (OPC-UA, Modbus, Kafka, soak test)
- [ ] Increment 11 — SECS/GEM track (HSMS spike → codec → GEM host; parallel from Inc. 6 onward)

## Backlog / unscheduled

- [ ] **Standing item**: update `docs/User-Guide.md`, `docs/Admin-Guide.md`, `README.md`, and CLAUDE.md's "Current state" at the end of every increment (now also a CLAUDE.md ground rule; guides currently describe the state after Increment 6; NFR-310/320 require them complete and offline-available by 1.0)

- [ ] MQTT real-broker integration test (`tests/e2e/mqtt_itest_test.go`) is written and correct but has never actually run in this development environment: `docker pull eclipse-mosquitto:2` hangs/fails due to a local Docker Hub connectivity issue (confirmed via a 25s alarm-gated pull returning exit 144). The itest infrastructure itself is proven correct by the sibling SQL itest, which runs a real `postgres:16-alpine` container successfully (that image was already cached locally). Run the MQTT itest in an environment with working Docker Hub access before relying on it in CI.
- [ ] Increment 6's Development-Plan "done when" line calls for a demo flow running "24 h unattended" — `examples/mqtt-sensor-to-postgres.flow.json` exists and is schema-valid (validated via a real control-plane `POST .../flows` call) but the actual 24-hour unattended soak run against live MQTT/Postgres backends is an operational deliverable that was not performed in this environment (needs a real broker/database plus a runtime left running — a good candidate for a scheduled/background task once Docker Hub access or external MQTT/Postgres endpoints are available)
- [ ] CON-140 "test connection" only implements real checks for `mqtt` and `postgres` (the two connector types with a concrete point-to-point endpoint worth probing in isolation); every other connection type reports "no live test available for this connection type" rather than failing — revisit as new connector families with a natural single-endpoint check land (e.g. OPC-UA in Increment 10)
- [ ] The new project-page Connections section (`ui/src/pages/ProjectDetail.tsx`) is a minimal first cut: create/list/delete/test only, no edit-in-place, no credential-attach UI (credentials still only manageable via REST), and the "config" field is a raw JSON textarea rather than a schema-driven form — revisit once node config schemas grow a `connectionType` concept the UI can key off of for a nicer picker
- [ ] Equipment simulator selection for SECS/GEM testing (Increment 11 prerequisite)
- [ ] Usability test participants for NFR-300 (first-flow-in-15-minutes criterion)
- [ ] Runtime↔control-plane gRPC channel currently dials with insecure credentials (walking-skeleton placeholder) — add TLS per Architecture §2.5/ADR-007 before edge runtimes (Increment 9) connect over untrusted networks
- [ ] `engine/datagram.Datagram.Clone` deep-copies Tags and small binary payloads but not generic map/slice `Payload.Value` — a node that mutates a map payload in place could leak the mutation across fan-out branches sharing that map; the `set` node works around this locally (its own deep-copy) but the general `Clone` gap remains (DGM-120/BUS-140)
- [ ] Flow-File-Format §7 rule 3 (node-config JSON-Schema validation) still not implemented — `engine/flow.Validate` covers ids/wires/mode/registered-types only; rule 2 (connection-ref resolution) is now moot for control-plane-issued deploys since flows there reference control-plane connection ids, but full validation still needs the node-manifest schema system (SDK track)
- [ ] ENG-150 resource guardrails only partially covered so far (bounded queues via `bus.Wire` capacity/overflow, panic recovery via ARC-150); script CPU/time/memory limits need the goja sandbox (Increment 7)
- [ ] ERR-120 (flow-level error handler) and ERR-130 (dead-letter topic) are not implemented — only the per-node ERR-100 policy (fail/retry/errorPort/discard) exists
- [ ] `registry.Service.DeployFlow` broadcasts every deploy to every connected runtime — Flow-File-Format's `runtimeAssignment`/UI-220 (deploy to a specific runtime/edge group) isn't implemented; needs real fleet/group targeting in Increment 9
- [ ] SEC-100 P2 items deferred: TOTP 2FA, OIDC/SAML SSO, LDAP/AD sync; SEC-110 custom granular roles (P2); SEC-120 external vault integration and a KEK-rotation admin operation (the versioned envelope mechanism that enables rotation exists, but nothing yet re-wraps existing DEKs under a new KEK)
- [ ] API-110's WebSocket channel now exists for live debugging (`/ws/debug`, Increment 5) but not for deploy-status/runtime-health push — those are still poll-only (GET /runtimes etc.); consider folding them into the same channel or a sibling one later
- [ ] API-120 versioning/deprecation policy and scoped API keys (as opposed to session tokens) not implemented
- [ ] VCS-120 (git integration) and VCS-150 (deployment pipeline promotion with approval gates) are P2, deferred
- [ ] Increment 4 (editor) scope explicitly excluded per Development-Plan's "subflow-less editing": UI-140 (subflows). Also not implemented (not listed in the Increment 4 bullet, deferred to their own later work): UI-180 auto-layout/printable docs (P2), UI-220 deploy-target/runtime-group selection (Increment 9 fleet work), UI-230 concurrent-editing presence (P2), UI-320 accessibility audit (P2), UI-330 onboarding tutorial + template gallery, UI-150 visual groups/sticky notes, UI-130 quick-insert-on-wire (the live-throughput-label half of UI-130 is now done via DBG-120's wire counters)
- [ ] UI-120's "last-value indicator" directly on a canvas node (as opposed to opening its Inspector tab) is still not built; live datagrams/sec and wire counters are covered by DBG-120's edge labels
- [ ] UI-200's "scope choice: full / modified flows only / modified nodes only" isn't a UI toggle — the editor always deploys one flow at a time (matching the REST API's per-flow deploy endpoint); modified-nodes-only behavior already happens automatically server-side via ENG-140 hot deploy regardless of what the UI offers
- [ ] Development-Plan Increment 4's "usability check with one target-persona user" is a human deliverable — not something an agent can perform; golden path (build + deploy inject→set→debug-log in the browser) was verified thoroughly via manual browser testing instead, but the actual usability test with a real target-persona user is still outstanding
- [ ] Automated browser verification of drawing a NEW wire by dragging (React Flow's pointer-capture-based connection gesture) didn't work via synthetic PointerEvents in headless testing — a testing-tool limitation, not a demonstrated app defect; the underlying `onConnect`/`reconnectEdge` store logic is unit-tested, and loading/saving/deploying a flow with pre-existing wires was verified live end to end. Worth a follow-up manual check in a real (non-headless) browser
- [ ] `engine/flow.Deployment` reconciles against a single `*FlowFile` per `Deploy` call and has no concept of "which flow" beyond the last one deployed — a runtime that received two different flows would tear down the first one's nodes when the second is deployed (its node ids aren't in the new file's graph). Today's control plane broadcasts one flow to every runtime, so this hasn't bitten yet, but real multi-flow-per-runtime (implied by Increment 9's fleet/group work) needs `Deployment` to key its reconciliation by flow id first
- [ ] DBG-130's data pinning stores a sample per (flow, node, port) and shows it in that node's own Inspector tab, but does not yet feed pinned values into *downstream* nodes' config forms (the Development-Plan's "so downstream configuration shows realistic values without live sources") — that needs SchemaForm/expression-editor integration, natural to pair with Increment 7's expression support (MAP-130)
- [ ] DBG-150 (lineage view) and DBG-160 (breakpoints) are P2/SHOULD per the requirements spec and were correctly out of Increment 5's scope; not implemented
- [ ] DBG-140 (execution history for triggered workflows) is Increment 8's territory (needs ENG-130 triggered mode first), not Increment 5's; not implemented
