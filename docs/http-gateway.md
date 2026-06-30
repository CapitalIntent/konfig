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
  is treated as `{}`.
- **Auth**: `Authorization: Bearer <token>` (unless `--http-insecure`).
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

## CORS

When `--http-cors-allow-origin` is set, the gateway:

- echoes it in `Access-Control-Allow-Origin` on every response, and
- answers `OPTIONS` preflights with `204` plus
  `Access-Control-Allow-Methods: POST, OPTIONS` and
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

## Out of scope

`Subscribe` / `SubscribeSecrets` streaming (separate SSE subtask); same-port
multiplexing with gRPC; strict `pbjson` proto3-JSON; prod manifest wiring
(Phase 6).
