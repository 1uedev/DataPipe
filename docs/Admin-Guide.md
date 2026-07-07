# DataPipe — Administrator Guide

**Covers:** development state after Increment 9 · **Audience:** operators of a DataPipe installation
**Components:** control plane (REST API + gRPC registry), runtime (flow engine), PostgreSQL or SQLite, editor UI.

## 1. Installation

### 1.1 Docker Compose (recommended for evaluation)

```bash
git clone https://github.com/1uedev/DataPipe.git && cd DataPipe
make dev        # compose up: postgres + controlplane + runtime, then starts the UI dev server
```

Services and ports (see `deploy/docker-compose.yml`):

| Service | Port(s) | Purpose |
|---|---|---|
| controlplane | 8080 (HTTP/REST), 9090 (gRPC) | API, auth, flow store; health at `GET /healthz` |
| runtime | 8082→8081 (HTTP) | flow engine; health at `GET /healthz`; registers itself at controlplane:9090 |
| postgres | 5432 | system store |
| UI dev server | 5173 | editor; proxies `/api/v1` to :8080 |

**Change the dev-only secrets before any non-local use** — the compose file ships a sample `DATAPIPE_MASTER_KEY` and admin password for `make dev` convenience only.

### 1.2 Bare binaries (SQLite, no Docker)

Both binaries are static Go builds (`make build`). The control plane picks its database from `DATABASE_URL`: a `postgres://` URL selects PostgreSQL, anything else (a file path, e.g. `/var/lib/datapipe/datapipe.db`) selects embedded SQLite — suitable for small single-host installs. Schema migrations run automatically at startup on both backends.

## 2. Configuration reference (environment variables)

### Control plane

| Variable | Default | Meaning |
|---|---|---|
| `CONTROLPLANE_HTTP_ADDR` | `:8080` | REST listen address |
| `CONTROLPLANE_GRPC_ADDR` | `:9090` | runtime registry listen address |
| `DATABASE_URL` | local Postgres DSN | `postgres://…` = PostgreSQL, else SQLite file path |
| `DATAPIPE_MASTER_KEY` | *(required)* | base64, 32 bytes — AES-256 KEK for credential envelope encryption (SEC-120). Generate: `openssl rand -base64 32`. **Losing it makes all stored credentials undecryptable; store it in a secret manager and back it up separately from the DB** |
| `DATAPIPE_ADMIN_USERNAME` | `admin` | bootstrap System Admin (created if missing at startup) |
| `DATAPIPE_ADMIN_PASSWORD` | *(unset)* | bootstrap admin password; if unset, no bootstrap user is created |

### Runtime

| Variable | Default | Meaning |
|---|---|---|
| `RUNTIME_HTTP_ADDR` | `:8081` | health endpoint |
| `CONTROLPLANE_GRPC_ADDR` | `localhost:9090` | where to register (runtime dials out — firewall friendly) |
| `RUNTIME_ID` | random UUID | stable id; set it explicitly so the runtime keeps its identity across restarts |
| `RUNTIME_ENROLL_TOKEN` | *(unset)* | per-device credential for fleet enrollment (EDGE-120/ARC-210, §8.1); leave unset for the walking-skeleton no-token path (fine for a single local/dev server) |
| `RUNTIME_DATA_DIR` | `./data` | base directory for `onError:"storeForward"` durable per-node queues (EDGE-130, §8.4); needs to be a real persistent volume/disk on an edge device, not tmpfs |

**Security note:** the runtime↔control-plane gRPC channel still uses insecure *transport* credentials (walking-skeleton state, tracked in TODO.md) even though Increment 9 added *identity* authentication via enrollment tokens (§8.1) — a token could still be sniffed off an untrusted network. Until TLS lands (planned before edge rollout, per Architecture §2.5), only run both on the same host or a trusted network segment, even for enrolled edge devices.

## 3. User and permission management

Authentication is local accounts (bcrypt-hashed passwords) with opaque bearer session tokens; only the token hash is stored. SSO (OIDC/SAML) and 2FA are on the roadmap, not built.

* Create users: `POST /api/v1/users` (System Admin only). There is no self-registration.
* Project membership and roles: `PUT /api/v1/projects/{id}/members/{userId}` with one of `viewer`, `operator`, `editor`, `project-admin`. Roles are strictly ordered; System Admin bypasses project scoping.
* A dedicated admin UI is not built yet — user/member management is done via the REST API (see `docs/api/openapi.yaml`; all endpoints work with `curl` and a session token from `POST /auth/login`).

## 4. Credentials and connections

Connection definitions and credentials are project-scoped (`/projects/{id}/connections`, `/projects/{id}/credentials`). Credential values are envelope-encrypted: each value gets its own random DEK, wrapped by the versioned master KEK (`DATAPIPE_MASTER_KEY`). Values are **write-only by construction** — no API response ever contains a decrypted value, and exports never include secrets. KEK rotation (re-wrapping DEKs under a new key version) is prepared in the data model but the admin operation is not implemented yet.

**How runtimes get credentials** (since Increment 6): deploy pushes never contain credential values. A connector node requests its decrypted connection config on demand over the runtime-initiated `ResolveConnection` gRPC call, and re-resolves on every reconnect — so rotating a credential in the control plane takes effect on the next reconnect without redeploying flows.

**Connection testing**: `POST /connections/{id}/test` (also a button in the project UI) performs a real bounded connect attempt from the control plane. Implemented for `mqtt` and `postgres`; other types report "no live test available".

## 5. Audit log

Every security-relevant action (logins, user/permission changes, credential writes, flow deploys, …) is appended to a hash-chained audit log: each entry carries a hash over its content plus the previous entry's hash, so any historical edit or deletion is detectable. Read it via `GET /api/v1/audit-log` (System Admin). Chain verification runs via the built-in `Verify` routine; a CLI wrapper and SIEM export are planned.

## 6. Flow lifecycle operations

* Every deploy validates the flow with the same validator the runtime uses, then snapshots an immutable version (`GET /flows/{id}/versions`); rollback with `POST /flows/{id}/versions/{v}/rollback`.
* Deploys are pushed to connected runtimes over a server-streaming gRPC channel; the runtime hot-swaps only the affected nodes (ENG-140) — untouched nodes keep running.
* `GET /api/v1/runtimes` lists connected runtimes, live health, and fleet group. A deploy with no `runtimeAssignment.group` in its content still goes to **all** connected runtimes (unchanged default); one with a group set only reaches runtimes enrolled into that group — see §8.2.
* CLI: `datapipe deploy <flow.json> [-for <duration>]` deploys a flow file directly to a runtime (developer tool); `datapipe version` prints the build version.
* **Crash recovery** (ERR-150): when a runtime (re)connects, the control plane immediately re-pushes every currently-deployed flow to it — no manual redeploy needed after a runtime restart. Any execution that was still `running`/`waiting` on a runtime whose connection just dropped is automatically marked `crashed` in its execution history (visible, re-runnable), rather than sitting "running" forever.

## 7. Triggered workflows: execution history and dead letters

A flow whose entry node is a trigger (HTTP In, Error Trigger) produces one tracked **execution** per fire, stored durably in the control plane's database (`executions`, `execution_node_io` tables) — separate from, and never sampled/dropped like, the live debug channel. REST surface: `GET /flows/{id}/executions` (filter by `status`), `GET /executions/{id}` (full per-node trace), `POST /executions/{id}/rerun` (`{"from":"start"}` or `{"from":"node","nodeId":"..."}`, Operator+), `POST /executions/{id}/cancel` (Operator+). A datagram a node couldn't deliver (error policy resolved to fail/discard, or a TTL expiry) is stored as a **dead letter** (`dead_letters` table): `GET /flows/{id}/dead-letters`, `POST /dead-letters/{id}/reinject` (Operator+), `DELETE /dead-letters/{id}`.

Transport: a second bidirectional gRPC stream, `EventChannel` (alongside `DebugChannel`), carries execution/dead-letter events from the runtime and re-run/cancel/reinject commands back down — opened once per runtime connection, same as `DeployStream`/`DebugChannel`.

**Flow-level error handling** (ERR-120): a project can designate a default error-handler flow (`PATCH /projects/{id}` with `defaultErrorFlow`); a flow can override it (`settings.errorFlow` in its content). Both are only meaningful if the handler flow is deployed to the same runtime as the flow producing errors — genuine cross-runtime error routing needs the multi-flow-per-runtime work tracked in TODO.md.

**Concurrency and timeouts**: a triggered flow's `settings.maxConcurrency`/`concurrencyPolicy` (`queue`|`reject`) and `executionTimeoutMs` are enforced runtime-side per flow, with no control-plane configuration needed beyond the flow content itself.

## 8. Fleet management and edge runtimes

### 8.1 Enrollment (EDGE-120/ARC-210)

`POST /runtime-groups` creates a named fleet group; `POST /runtime-enroll-tokens` (System Admin) issues a per-device credential — a 32-byte random token, returned in plaintext exactly once in that response and stored control-plane-side only as its SHA-256 hash (`runtime_enroll_tokens.token_hash`), the same pattern session tokens already use. Optionally pre-assign the token to a group so the device lands there automatically.

Start the edge runtime with `RUNTIME_ENROLL_TOKEN=<token>` (and a stable `RUNTIME_ID`). On `Register`, the control plane validates the token and creates a `devices` row (`runtime_id`, `kind`, `group_name`, `enroll_token_id`) if this is that runtime's first-ever registration; every *subsequent* `Register` for that same `runtime_id` must present the identical token or is rejected — enrollment, once established, is a real per-device credential check on every reconnect, not just a one-time bootstrap. A `runtime_id` that presents no token at all is still accepted as long as it has never enrolled (the pre-Increment-9 no-token path stays available for a single local/dev server). `DELETE /runtime-enroll-tokens/{id}` revokes a token (blocks future use); it does not retroactively un-enroll a device that already used it — there is no device-delete endpoint yet (tracked in TODO.md).

`PATCH /runtimes/{id}` (System Admin) renames a device or (re)assigns its group directly, independent of enrollment — useful for grouping a runtime that registered without a token.

### 8.2 Deploy targeting (UI-220)

A flow's `runtimeAssignment.group` (set from the editor's deploy-target dropdown, or directly in the flow JSON) restricts which runtimes a deploy reaches: `registry.Service.DeployFlow` and the automatic re-push a (re)connecting runtime gets (§6's crash recovery) both filter by the runtime's current group membership. `""`/unset means every connected runtime, same as before Increment 9. Group membership can change at any time via `PATCH /runtimes/{id}`; it takes effect on the next deploy or reconnect, not retroactively on already-running nodes.

**Caveat:** this does not add multi-flow-per-runtime support — a runtime still applies one deployed flow at a time regardless of how many different flows target its group (tracked in TODO.md).

### 8.3 Fleet health

`GET /api/v1/runtimes` reports each runtime's live CPU%/memory (self-sampled via `syscall.Getrusage`/`runtime.MemStats`, sent on every `Heartbeat`) and its current flow count — all in-memory, refreshed continuously, never persisted (persisted fleet state is only the admin-configured enrollment/group data in §8.1). A runtime with no open `DeployStream` reports `online: false` and no health snapshot.

### 8.4 Store-and-forward (EDGE-130)

A node's `errorPolicy.onError: "storeForward"` (set in the flow JSON; no config-panel UI yet) durably queues datagrams to local disk under `RUNTIME_DATA_DIR/storeforward/<flowId>/<nodeId>/` instead of failing/discarding when its destination is unreachable, and drains them in order once it recovers — including surviving the runtime process restarting while the backlog is still queued (verified by `engine/flow.TestEDGE130_StoreForwardQueueSurvivesDeploymentRestart`, race-detector clean). `storeForward.maxSizeMb`/`maxAgeSec` bound the queue; oldest entries are dropped past either limit (BUS-110 "nothing buffers unboundedly"), and drops are logged. This is what lets an edge flow "run autonomously without control-plane connection" (EDGE-130) — the queue and its drain loop live entirely inside the runtime process and need no round trip to the control plane to operate. Make sure `RUNTIME_DATA_DIR` points at real persistent storage on an edge device, not tmpfs, or the queue won't survive a reboot.

### 8.5 Edge build

`make build-edge` (from the repo root) cross-compiles a static, `CGO_ENABLED=0` runtime binary for `linux/arm64` by default (`EDGE_GOARCH=amd64` for x86-64 edge boxes), written to `dist/datapipe-runtime-linux-<arch>` — roughly 22 MB, no libc dependency, so it runs unmodified on a minimal/musl-based edge image. **Honestly unverified in this environment:** the binary has been confirmed to actually be a `linux/arm64` ELF (via `file`) and cross-compiles cleanly, but has never run on physical ARM64 hardware, and no real prolonged (30-minute) network partition was exercised — only a simulated "destination unreachable" and a full local process restart, which exercise the same store-and-forward code path but aren't the same as real hardware over a real outage. See TODO.md.

## 9. Backup and restore

Back up three things: the database (`pg_dump` for PostgreSQL, or a copy of the SQLite file taken while the control plane is stopped), the `DATAPIPE_MASTER_KEY` (separately, in a secret manager — a DB backup without the key has unrecoverable credentials), and your `deploy/` configuration. Restore = restore DB, set the same master key, start the control plane (migrations verify the schema), restart runtimes (they re-register and receive their flows on the next deploy).

## 10. Live debug channel

Since Increment 5 the editor's live inspection runs over a WebSocket at `/ws/debug` on the control plane (protocol documented in `docs/api/debug-websocket.md`; the session token travels as a query parameter because the WS handshake cannot carry the auth header). Access is gated at **Operator or higher** per project — Viewers cannot see payloads. The runtime only captures and forwards debug data for flows someone is actually watching, the live stream is rate-limited (default 20 events/s per node) and payloads over 4 KiB are truncated before relay, so debugging a high-volume flow does not overload runtime, control plane, or browser. Wire counters remain exact regardless of sampling.

## 11. Monitoring and troubleshooting

| Symptom | Check |
|---|---|
| UI shows "no runtime connected" on deploy (HTTP 409) | Is the runtime process up? `GET :8082/healthz`; does its `CONTROLPLANE_GRPC_ADDR` point at the control plane? `GET /api/v1/runtimes` should list it |
| Login fails after fresh install | Was `DATAPIPE_ADMIN_PASSWORD` set at first startup? Without it no bootstrap admin exists |
| Control plane won't start | `DATAPIPE_MASTER_KEY` missing/not valid base64-32-bytes; or `DATABASE_URL` unreachable |
| Deploy rejected (HTTP 400) | Response body lists validation errors (broken wires, unknown node types, mode violations) |
| Where is flow output? | In the editor: node Inspector (Inspect tab) and the debug sidebar show live data (Operator+). Runtime console logging is an opt-in setting on the Debug Log node; `docker compose logs -f runtime` still shows engine logs |
| Live inspection shows nothing | Is the user at least Operator in the project? Does `/ws/debug` reach the control plane (reverse proxies must allow WebSocket upgrade)? |
| Connection test fails | The response contains the real dial error (e.g. `connection refused`); verify host/port from the control plane's network perspective — the test runs there, not on the runtime |

Health endpoints: control plane `:8080/healthz`, runtime `:8081/healthz` (compose maps it to 8082). Prometheus metrics (OBS-100) are specified but not implemented yet.

## 12. Upgrades

Pull the new version, rebuild (`docker compose build` or `make build`), restart control plane first (runs migrations), then runtimes. Flow definitions and versions are forward-compatible per the flow file format's `formatVersion` rules. Take a backup before upgrading.
