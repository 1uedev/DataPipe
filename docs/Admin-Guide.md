# DataPipe — Administrator Guide

**Covers:** development state after Increment 6 · **Audience:** operators of a DataPipe installation
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

**Security note:** the runtime↔control-plane gRPC channel currently uses insecure transport credentials (walking-skeleton state, tracked in TODO.md). Until TLS lands (planned before edge rollout, per Architecture §2.5), only run both on the same host or a trusted network segment.

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
* `GET /api/v1/runtimes` lists connected runtimes. Currently every deploy goes to **all** connected runtimes; per-runtime/group targeting arrives with fleet management (Increment 9).
* CLI: `datapipe deploy <flow.json> [-for <duration>]` deploys a flow file directly to a runtime (developer tool); `datapipe version` prints the build version.

## 7. Backup and restore

Back up three things: the database (`pg_dump` for PostgreSQL, or a copy of the SQLite file taken while the control plane is stopped), the `DATAPIPE_MASTER_KEY` (separately, in a secret manager — a DB backup without the key has unrecoverable credentials), and your `deploy/` configuration. Restore = restore DB, set the same master key, start the control plane (migrations verify the schema), restart runtimes (they re-register and receive their flows on the next deploy).

## 8. Live debug channel

Since Increment 5 the editor's live inspection runs over a WebSocket at `/ws/debug` on the control plane (protocol documented in `docs/api/debug-websocket.md`; the session token travels as a query parameter because the WS handshake cannot carry the auth header). Access is gated at **Operator or higher** per project — Viewers cannot see payloads. The runtime only captures and forwards debug data for flows someone is actually watching, the live stream is rate-limited (default 20 events/s per node) and payloads over 4 KiB are truncated before relay, so debugging a high-volume flow does not overload runtime, control plane, or browser. Wire counters remain exact regardless of sampling.

## 9. Monitoring and troubleshooting

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

## 10. Upgrades

Pull the new version, rebuild (`docker compose build` or `make build`), restart control plane first (runs migrations), then runtimes. Flow definitions and versions are forward-compatible per the flow file format's `formatVersion` rules. Take a backup before upgrading.
