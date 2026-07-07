# DataPipe — Expression Language (MAP-130/150)

**Status:** Implemented (`engine/expr`), Increment 7 · **Basis:** Requirements spec §8 (MAP-130/140/150), `docs/Flow-File-Format.md` §2

One documented syntax is used everywhere a node parameter can be a literal or a computed value: JavaScript, evaluated by an embedded, sandboxed interpreter ([goja](https://github.com/dop251/goja)) with no filesystem or network access — scripts cannot perform I/O regardless of what they try to call (SEC-150).

## 1. Two conventions

Flow-File-Format.md §2 defines these; every node built on `engine/expr` honors them identically.

### Whole-value expressions: `={{ ... }}`

A JSON string field whose value starts with `={{` and ends with `}}` is evaluated as one JavaScript expression; the result (any JSON type — string, number, object, array, boolean, null) replaces the field. Everything else is a literal.

```jsonc
{ "value": "={{payload.value}}" }       // expression: reads payload.value
{ "deadband": 0.5 }                      // literal number
{ "label": "line-3" }                    // literal string
```

This is applied per-field (`expr.ResolveValue`) or recursively through an entire config object (`expr.ResolveDeep`) — every nested string leaf that matches the convention is evaluated independently.

### Mixed templates: `{{ ... }}` (no `=` prefix)

Used where a node builds a STRING from literal text plus computed pieces (e.g. `template`, a URL, a topic name) — every `{{ expr }}` placeholder is evaluated and spliced into the surrounding text (`expr.RenderTemplate`). Because each placeholder is a full JavaScript expression, this is "logic-capable" without a second templating grammar: ternaries, `.map()/.join()`, string concatenation all just work.

```
"sensor {{tags.line}}: {{payload.value}} ({{payload.value > 20 ? 'high' : 'ok'}})"
```

## 2. Context available to every expression

| Binding | What it is |
|---|---|
| `payload` | The current datagram's payload value (any JSON type) |
| `header` | The datagram header as a plain object: `id`, `correlationId`, `causationId`, `timestamp`, `sourceTimestamp`, `source`, `schemaRef`, `contentType`, `quality`, `priority`, `ttl`, `tags` |
| `tags` | Shorthand for `header.tags` (`map[string]string`) |
| `env` | Process environment variables (read-only) |
| `flow` | Flow-scoped state: `flow.get(key)` / `flow.set(key, value)` (backed by `engine/ctxstore`, PROC-410) |
| `global` | Global-scoped state: `global.get(key)` / `global.set(key, value)` |

A node type that also needs **node**-scoped state (not part of the standard expression context) exposes its own extra binding — e.g. the `script` node's `state.get`/`state.set` (PROC-410's third scope).

`flow`/`global` are only writable when the expression runs inside a live Deployment (or a test that explicitly attaches a store via `flow.WithContextStore`); calling `.set()` without one throws a catchable JavaScript error rather than silently discarding the write.

## 3. Standard function library

Beyond native JavaScript (`Math`, `String`/`Array`/`Object` prototypes, regex literals, `JSON`, ternaries, etc.), these globals are bound:

### `dt` — date/time with time zones
- `dt.now()` → current epoch milliseconds
- `dt.nowISO()` → current time as an ISO-8601 UTC string
- `dt.parseISO(s)` → epoch milliseconds
- `dt.format(epochMs, layout, tz)` → formatted string in the given IANA time zone (default UTC). `layout` uses moment.js-style tokens: `YYYY MM DD HH mm ss SSS Z` (e.g. `"YYYY-MM-DD HH:mm:ss"`)
- `dt.addMs(epochMs, deltaMs)`, `dt.diffMs(a, b)`

### `stats` — statistics
- `stats.sum(arr)`, `stats.mean(arr)`, `stats.min(arr)`, `stats.max(arr)`
- `stats.stddev(arr)` — **sample** standard deviation (n-1 denominator)
- `stats.percentile(arr, p)` — linear interpolation between closest ranks (0 ≤ p ≤ 100)

### `conv` — explicit type casting (MAP-150)
- `conv.toNumber(v)`, `conv.toString(v)`, `conv.toBool(v)` — **throw** on an unrecognized value rather than silently returning `null`/`NaN`, per MAP-150's "cast failures follow the node's error policy"
- `conv.base64Encode(s)` / `conv.base64Decode(s)`
- `conv.epochToISO(epochMs)` / `conv.isoToEpoch(s)`

### `hash` — hashing
- `hash.md5(s)`, `hash.sha1(s)`, `hash.sha256(s)` — hex-encoded digests

## 4. Sandboxing and resource limits (SEC-150, ENG-150)

- No `require`, `fetch`, `process`, filesystem, or network globals are ever bound — verified by dedicated tests (`TestSEC150_NoFilesystemOrNetworkGlobalsBound`).
- Every evaluation is bounded by a timeout (`expr.DefaultTimeout` = 2s, overridable per call site) enforced via goja's `Interrupt`; an expression that runs long is aborted with an error rather than hanging the node's goroutine forever.
- Evaluation also aborts promptly if the calling context is cancelled (e.g. flow redeploy/stop).
- There is currently no memory ceiling on a single expression's execution (a genuinely pathological allocation loop is bounded only by the timeout, not by memory) — see TODO.md.

## 5. Performance note

Compiling JavaScript source (`expr.Compile`) is more expensive than running it. Node types that evaluate the SAME expression on every datagram (calculator, switch, filter, merge, delay, lookup, window-aggregate) compile once at construction and reuse one `expr.Runtime` per node instance across every `Process` call — safe because the engine already drives one node instance's calls serially from its own goroutine. `expr.Eval`/`ResolveValue`/`ResolveDeep` are one-shot convenience wrappers (compile-and-discard) for less hot-path uses.

## 6. Node types built on this engine

`script` (PROC-100, full program + `emit`/`console`/`state`), `calculator` (PROC-200), `template` (PROC-130), `switch` (PROC-300, boolean predicates — comparisons/regex/type checks/quality/tag matches are all just expressions), `filter` (PROC-310, predicate or deadband/groupBy expressions), `merge` (PROC-320, join keys), `delay` (PROC-350, delay/groupBy expressions), `lookup` (PROC-400, key expression), `state` (PROC-410, literal-or-expression values), `window-aggregate` (PROC-210, groupBy expression).
