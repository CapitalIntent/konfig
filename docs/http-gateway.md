# HTTP/JSON gateway (CU-86ahrwd70)

Browsers and non-gRPC clients cannot speak gRPC. The HTTP/JSON gateway is a
small `axum` server that transcodes JSON ⇄ protobuf for the full **unary**
`konfig.v1.KonfigService` surface, so a plain `POST` with a JSON body reaches
the same handlers as a gRPC call.

It is a thin transcoder, not a second implementation: it shares the one
`KonfigServer` instance, so every gate — graceful drain, per-tenant
authorization, quotas, audit logging, and metrics — applies identically to JSON
callers.

## Enabling

The gateway is **off by default**. Turn it on with `--http-addr` (a sibling of
the gRPC `--grpc-addr`); see [`docs/configuration.md`](configuration.md) for the
full flag/env table.

```sh
konfig --name app-config \
  --http-addr 0.0.0.0:8080 \
  --http-auth-token "$KONFIG_HTTP_TOKEN" \
  --http-cors-allow-origin https://backstage.example
```

Because the gateway exposes writes and secret reads over a plaintext port, a
**bearer token is required**. Startup fails fast if `--http-addr` is set without
`--http-auth-token`, unless you explicitly pass `--http-insecure=true` (mirrors
the `--tls` fail-safe).

## Request shape

- **Method**: `POST`
- **Path**: `/konfig.v1.KonfigService/<RpcName>` (e.g. `/konfig.v1.KonfigService/Get`)
- **Body**: proto3-JSON of the request message, using the proto's snake_case
  field names. An omitted field defaults to the proto zero value; an empty body
  is treated as `{}`. The body is capped at **4 MiB** (matching the gRPC
  server's default decode limit); a larger body is rejected with `413`.
- **Auth**: `Authorization: Bearer <token>` (unless `--http-insecure`). The
  `Bearer` scheme name is case-insensitive.
- **Response**: proto3-JSON of the response message on success; on error,
  `{"code": "<grpc_code>", "message": "<detail>"}` with a mapped HTTP status.

### gRPC code → HTTP status

| gRPC code | HTTP |
|-----------|------|
| `OK` | 200 |
| `InvalidArgument` | 400 |
| `Unauthenticated` | 401 |
| `PermissionDenied` | 403 |
| `NotFound` | 404 |
| `AlreadyExists` | 409 |
| `FailedPrecondition` | 412 |
| `ResourceExhausted` | 429 |
| `DeadlineExceeded` | 504 |
| `Unavailable` | 503 |
| `Unimplemented` | 501 |
| everything else (`Internal`, `Unknown`, …) | 500 |

The gateway itself also returns a few statuses before a handler runs: `401`
(missing/invalid bearer token), `404 not_found` (unknown method name), `405
method_not_allowed` (a verb other than `POST`/`OPTIONS`), and `413` (body over
4 MiB). The real-but-non-transcodable streaming RPCs return `501` (see below) —
distinct from the `404` for a method that does not exist at all.

## Methods

Full unary parity. The two finite server-streams (`GetAll` / `GetAllSecrets`)
are drained into a JSON array. The infinite streams (`Subscribe` /
`SubscribeSecrets`) return `501` — use the SSE endpoint for those.

The examples below assume `TOKEN=$KONFIG_HTTP_TOKEN` and a gateway on `:8080`.

### Get

```sh
curl -sS http://konfig:8080/konfig.v1.KonfigService/Get \
  -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"namespace":"default","name":"app-config"}'
# → {"namespace":"default","name":"app-config","schema_version":3,
#    "content_json":"{...}","resource_version":"12345","age_ms":42,"stale_since_ms":-1}
```

### GetAll (streamed → JSON array)

```sh
curl -sS http://konfig:8080/konfig.v1.KonfigService/GetAll \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"namespace":"default"}'
# → [ {Config…}, {Config…}, … ]
```

### Apply

```sh
curl -sS http://konfig:8080/konfig.v1.KonfigService/Apply \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"namespace":"default","name":"app-config","yaml_content":"schema_version: 4\ncontent:\n  feature_x: true\n"}'
# → {"resource_version":"12346"}
```

### BatchApply

```sh
curl -sS http://konfig:8080/konfig.v1.KonfigService/BatchApply \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"items":[{"namespace":"default","name":"a","yaml_content":"schema_version: 1\n"},
               {"namespace":"default","name":"b","yaml_content":"schema_version: 1\n"}]}'
# → {"results":[{"namespace":"default","name":"a","resource_version":"…"}, …]}
```

### DryRunApply

```sh
curl -sS http://konfig:8080/konfig.v1.KonfigService/DryRunApply \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"namespace":"default","name":"app-config","yaml_content":"schema_version: 5\n"}'
# → {"current_content_json":"{…}","proposed_content_json":"{…}",
#    "current_schema_version":4,"proposed_schema_version":5}
```

### Revert

```sh
curl -sS http://konfig:8080/konfig.v1.KonfigService/Revert \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"namespace":"default","name":"app-config","to_resource_version":"12000"}'
# → {"resource_version":"12347","schema_version":6}
```

### GetSecret

```sh
curl -sS http://konfig:8080/konfig.v1.KonfigService/GetSecret \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"namespace":"konfig-system","name":"db-creds"}'
# → {"namespace":"konfig-system","name":"db-creds","schema_version":2,
#    "data_json":"{\"user\":\"<base64>\"}","resource_version":"…","age_ms":5,"stale_since_ms":-1}
```

Secret values stay base64-encoded — the server does not decode them.

### GetAllSecrets (streamed → JSON array)

```sh
curl -sS http://konfig:8080/konfig.v1.KonfigService/GetAllSecrets \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"namespace":"konfig-system"}'
# → [ {SecretResponse…}, … ]
```

### ApplySecret

```sh
curl -sS http://konfig:8080/konfig.v1.KonfigService/ApplySecret \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"namespace":"konfig-system","name":"db-creds","yaml_content":"user: alice\npassword: s3cret\n"}'
# → {"resource_version":"…"}
```

The server base64-encodes the YAML map values before patching.

## Streaming (SSE)

The infinite `Subscribe` / `SubscribeSecrets` RPCs cannot collapse into one JSON
body, so they are served as [Server-Sent Events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events)
— an HTTP-native, one-way `text/event-stream` that any browser (`EventSource`)
or `curl` can read. These are a **thin presentation layer**: each endpoint
re-frames the *same* in-process broadcast fan-out the gRPC `Subscribe` clients
use (same replay, same resume, same lag handling) — no new watch logic.

- **Method/Path**: `GET /v1/configs/{namespace}/{name}/watch` and
  `GET /v1/secrets/{namespace}/{name}/watch` (one key per connection).
- **Auth**: the *same* uniform bearer gate as the rest of the gateway — applies
  to config **and** secret streams. `--http-insecure` bypasses both.
- **Readiness**: during cold start (before the watcher's first list warms the
  cache) the endpoint returns `503` + `Retry-After: 1`; retry shortly.

### Frame shape

Each change is one SSE frame:

```
event: MODIFIED
id: 12346
data: {"namespace":"default","name":"app-config","schema_version":4,"content_json":"{...}","resource_version":"12346","age_ms":5,"stale_since_ms":-1}
```

- `event:` is the change type — `SNAPSHOT` (the current state sent first),
  `ADDED`, `MODIFIED`, or `DELETED`.
- `id:` is the `resource_version`. The browser remembers the last `id:` it saw
  and echoes it back as the `Last-Event-ID` header on reconnect; the gateway
  maps that to `resume_resource_version` so you only replay what you missed.
- `data:` is the proto3-JSON `Config` (or `SecretResponse`, whose `data_json`
  stays base64 — same policy as `GetSecret`).
- A `: keep-alive` comment is sent every 15 s so idle connections survive proxy
  timeouts (the `EventSource` parser ignores comment lines).

### Terminal frames

Once bytes are flowing the HTTP status is locked at `200`, so a mid-stream fault
is surfaced as a final frame and then the stream ends:

- **Lag** (`event: RELOAD`) — the subscriber fell behind the broadcast ring
  (`RESOURCE_EXHAUSTED`). The client must **re-fetch** a fresh snapshot (e.g.
  reconnect, which replays a `SNAPSHOT`).
- **Error** (`event: error`) — any other gRPC error, carrying
  `{"code":"...","message":"..."}`.

On graceful drain (SIGTERM) the underlying subscriber closes cleanly
(end-of-stream, no terminal frame), so `EventSource` simply reconnects to a
healthy pod.

### Examples

```sh
# Watch one config (Ctrl-C to stop)
curl -N http://konfig:8080/v1/configs/default/app-config/watch \
  -H "Authorization: Bearer $TOKEN"

# Resume after a disconnect from the last resource_version you saw
curl -N http://konfig:8080/v1/configs/default/app-config/watch \
  -H "Authorization: Bearer $TOKEN" -H 'Last-Event-ID: 12346'

# Watch one secret (values stay base64)
curl -N http://konfig:8080/v1/secrets/konfig-system/db-creds/watch \
  -H "Authorization: Bearer $TOKEN"
```

### Known limitation: browser `EventSource` + auth

The browser `EventSource` API **cannot set an `Authorization` header**, so a
secured stream (the default) is not directly reachable from page JS. Options:

- Run the gateway **cluster-internal** with `--http-insecure` behind a trusted
  proxy that injects the token (recommended), or
- Front it with a server-side reverse proxy that adds the bearer header, or use
  a `fetch`-based SSE polyfill that can set headers.

A query-parameter token (`?access_token=…`) is a possible later follow-up but is
intentionally not implemented here (tokens in URLs leak into logs).

## REST GET (one-shot) + readiness

For clients that prefer polling over streaming, the gateway serves plain `GET`
aliases for the read RPCs (**configs only** — secret reads stay on the
audit-logged `POST` path):

- `GET /v1/configs/{namespace}/{name}` → unary `Get`; returns the JSON `Config`
  (`404` if absent). Same bearer gate as the rest of the gateway.
- `GET /v1/configs/{namespace}` → server-streaming `GetAll`; returns a JSON
  array **streamed one element at a time** (the whole namespace is never
  buffered in memory). Same bearer gate.
- Both answer a CORS preflight (`OPTIONS`) so a cross-origin browser `fetch`
  with an `Authorization` header works when `--http-cors-allow-origin` is set.

```sh
curl http://konfig:8080/v1/configs/default/app-config -H "Authorization: Bearer $TOKEN"
curl http://konfig:8080/v1/configs/default            -H "Authorization: Bearer $TOKEN"
```

### `/healthz` readiness

`GET /healthz` is the **unauthenticated** readiness probe for Kubernetes
(liveness/readiness probes carry no bearer token):

- `200 {"status":"ok","cache_ready":true}` once the config cache has completed
  its first list (the same gate the gRPC health check + SSE streams use).
- `503 {"status":"warming","cache_ready":false}` + `Retry-After: 1` while the
  cache is still warming.

```sh
curl http://konfig:8080/healthz    # no token needed
```

## CORS

When `--http-cors-allow-origin` is set, the gateway:

- echoes it in `Access-Control-Allow-Origin` on every response (plus
  `Vary: Origin`, so a shared cache never replays one origin's response to
  another), and
- answers `OPTIONS` preflights with `204` plus
  `Access-Control-Allow-Methods: GET, POST, OPTIONS` and
  `Access-Control-Allow-Headers: content-type, authorization`.

With the flag unset, no CORS headers are emitted — same-origin browser requests
only (the safe default).

## Security model

Read this before exposing the port.

- **Plaintext, no mTLS.** The gateway carries no client certificate, so calls
  reach konfig as the `anonymous` identity (`extract_identity` reads
  `peer_certs`). Under the default `KONFIG_AUTHZ_MODE=disabled` every method
  works; under `enforce`, anonymous is denied. This is a documented limitation,
  not a silent bypass — run the gateway **cluster-internal** behind a trusted
  caller. (A follow-up could let `extract_identity` honour an injected identity
  so an operator can grant the gateway an ACL identity.)
- **Bearer token required.** Because the surface includes writes (`Apply`,
  `BatchApply`, `Revert`, `ApplySecret`) and secret reads (`GetSecret`,
  `GetAllSecrets`), the bearer token is mandatory unless `--http-insecure=true`
  is explicitly passed. The token is compared in constant time.
- **Not in prod manifests.** The gateway is not wired into `infra/konfig`; it
  stays opt-in until Phase 6 deployment work exposes the port when needed.

## Draining (SIGTERM)

The gateway shares the gRPC server's drain signal. On SIGTERM it stops
accepting new connections and lets in-flight requests finish (instead of being
cut mid-response on process exit); requests still in flight during the drain
window get the same `UNAVAILABLE` → `503` the handlers already return while
draining.

## Out of scope

REST `GET` for **secrets** (kept on the audit-logged `POST`/gRPC path); same-port
multiplexing with gRPC; strict `pbjson` proto3-JSON; prod manifest wiring
(Phase 6).
