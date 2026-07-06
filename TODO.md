# TODO — Next Steps

Working queue for DataPipe. Top item is always next. Detail and acceptance criteria live in `docs/Development-Plan.md`; this file only tracks order and status. When a step is finished, move its entry to `DONE.md` with date and commit hash.

## Now

- [ ] **Increment 3 — Control plane core + REST API** (auth, RBAC, credentials, versions)

## Next (in order, from docs/Development-Plan.md)

- [ ] Increment 4 — Editor MVP (canvas, palette, schema-generated config panels, deploy)
- [ ] Increment 5 — Live debugging (inspector, debug sidebar, wire animation, data pinning)
- [ ] Increment 6 — First real connectors (MQTT, HTTP/REST, schedule, files, Postgres, bus topics)
- [ ] Increment 7 — Processor library P1 (script sandbox, window/aggregate, switch, expressions)
- [ ] Increment 8 — Triggered workflows (execution history, error flows, DLQ, re-run)
- [ ] Increment 9 — Edge runtime + fleet (enrollment, store-and-forward, runtime groups)
- [ ] Increment 10 — Remaining P1 connectors + hardening (OPC-UA, Modbus, Kafka, soak test)
- [ ] Increment 11 — SECS/GEM track (HSMS spike → codec → GEM host; parallel from Inc. 6)

## Backlog / unscheduled

- [ ] Equipment simulator selection for SECS/GEM testing (Increment 11 prerequisite)
- [ ] Usability test participants for NFR-300 (first-flow-in-15-minutes criterion)
- [ ] Runtime↔control-plane gRPC channel currently dials with insecure credentials (walking-skeleton placeholder) — add TLS per Architecture §2.5/ADR-007 before edge runtimes (Increment 9) connect over untrusted networks
- [ ] Confirm `.github/workflows/ci.yml` goes green on actual GitHub Actions end to end — run #3 caught a real gap (proto job's `buf generate` needs protoc-gen-go/protoc-gen-go-grpc on PATH, fixed in `14b7089`); still need to watch a full green run since CI itself hadn't been observed running remotely before this
- [ ] `engine/datagram.Datagram.Clone` deep-copies Tags and small binary payloads but not generic map/slice `Payload.Value` — a node that mutates a map payload in place could leak the mutation across fan-out branches sharing that map; the `set` node works around this locally (its own deep-copy) but the general `Clone` gap remains (DGM-120/BUS-140)
- [ ] Flow-File-Format §7 rules 2–3 (connection-ref resolution, node-config JSON-Schema validation) are not implemented yet — `engine/flow.Validate` only covers ids/wires/mode; land with the connection registry (Increment 3) and node-manifest schemas (SDK track)
- [ ] ENG-150 resource guardrails only partially covered so far (bounded queues via `bus.Wire` capacity/overflow, panic recovery via ARC-150); script CPU/time/memory limits need the goja sandbox (Increment 7)
- [ ] ERR-120 (flow-level error handler) and ERR-130 (dead-letter topic) are not implemented — Increment 2 only covers the per-node ERR-100 policy (fail/retry/errorPort/discard)
