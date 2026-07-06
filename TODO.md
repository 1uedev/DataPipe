# TODO — Next Steps

Working queue for DataPipe. Top item is always next. Detail and acceptance criteria live in `docs/Development-Plan.md`; this file only tracks order and status. When a step is finished, move its entry to `DONE.md` with date and commit hash.

## Now

- [ ] **Increment 2 — Flow model + engine lifecycle** (flow file round-trip, hot deploy, ERR-100)

## Next (in order, from docs/Development-Plan.md)

- [ ] Increment 3 — Control plane core + REST API (auth, RBAC, credentials, versions)
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
- [ ] Confirm `.github/workflows/ci.yml` goes green on actual GitHub Actions after first push with a workflow run (only verified locally so far)
