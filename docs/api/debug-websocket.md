# Live debugging WebSocket protocol (Increment 5)

DBG-100/110/120/170 need a live push channel from the browser's inspector
and debug sidebar to the control plane; REST/OpenAPI doesn't fit a
continuous stream, so this contract lives in its own document per
CLAUDE.md's "spec-first for contracts" rule — implementation must match
this, not invent fields.

## Endpoint

```
GET /api/v1/ws/debug?flowId=<flowId>&token=<sessionToken>
```

Upgrades to a WebSocket. `token` is the same opaque bearer session token
used for `Authorization: Bearer` elsewhere — passed as a query parameter
here only because browsers cannot set custom headers on the WebSocket
handshake. The caller must hold at least the `operator` project role on
`flowId`'s project (DBG-170/SEC-110: "Viewer ... no payload inspection
unless granted" — no granular grant mechanism exists yet, so `operator`+ is
the current bar). One socket subscribes to exactly one flow; open a second
socket to watch a second flow concurrently.

## Server → client messages

Every message is a JSON object with a `type` field.

```jsonc
// A node/wire/sidebar debug event (DBG-100/110).
{
  "type": "event",
  "event": {
    "id": "01H...",
    "flowId": "...",
    "nodeId": "...",
    "port": "out",
    "direction": "in" | "out" | "sidebar",
    "label": "",
    "timeUnixMs": 1730000000000,
    "datagramId": "01H...",
    "correlationId": "01H...",
    "causationId": "",
    "quality": "GOOD",
    "valueJson": "{\"temp\":42.5}",
    "truncated": false,
    "fullLength": 0
  }
}

// A periodic wire-metrics snapshot (DBG-120's live counters/rates).
{
  "type": "wireMetrics",
  "metrics": {
    "flowId": "...", "fromNode": "...", "fromPort": "out",
    "toNode": "...", "toPort": "in",
    "delivered": 1234, "dropped": 0
  }
}
```

On connect, the server immediately replays its cached recent history for
the flow (oldest first) before any new live events, so the inspector shows
data even if nothing has passed through since the socket opened (DBG-100:
"inspection works without redeploy").

`valueJson` is truncated above `debughub.MaxInlinePayloadBytes` (4096
bytes); `truncated`/`fullLength` tell the client to offer a "load full"
action, backed by `GET /flows/{flowId}/debug/events/{eventId}` (see
openapi.yaml).

## Client → server messages

None required for normal operation — subscribing is implicit in opening the
socket with `flowId`, and unsubscribing is implicit in closing it. The
socket is otherwise read-only from the client's perspective; sending any
message from the client is a no-op reserved for future use (e.g. DBG-160
breakpoints, P2, not implemented).

## Delivery guarantees

This channel is explicitly best-effort and lossy (DBG-170): if a browser
falls behind, the server drops the oldest unread item for that subscriber
rather than blocking. It is an observability view, not a data-integrity
path — BUS-110's backpressure guarantees apply only within the flow engine
itself, never to this channel.
