<p align="center">
  <img src="docs/img/switchboard.png" alt="Switchboard" width="420">
</p>

# Switchboard Sidecar

Switchboard is an AI infrastructure platform. The **Switchboard Sidecar** is its
deployable data-plane component: a Go inference sidecar with a Postgres-backed
Python/FastAPI control plane, targeting AWS ECS/Fargate.

It runs beside your application in the same task, exposes one OpenAI-compatible
endpoint on loopback, and routes to OpenAI, Anthropic or Gemini according to a
signed policy it cannot itself edit.

**Release status: production-oriented release candidate, not production-certified.** This is a fresh implementation of the Switchboard architecture and has not been verified for compatibility with any earlier version's implementation or persisted data. Read [validation](docs/VALIDATION.md) and [remaining gaps](docs/GAPS.md) before deployment, and the [unsupported request surface](docs/API.md#unsupported-request-surface) before assuming an OpenAI-compatible client will work unchanged.

## Try it

Docker is the only prerequisite. No AWS account, no provider API key, no cost:
the stack runs against a mock provider, and everything a real deployment needs —
an Ed25519 signing key, a database, a tenant, and a **signed routing policy** —
is generated for you.

```sh
make dev-up      # build, generate keys, migrate, provision, start
make dev-smoke   # send a real completion through the whole path
make dev-down    # remove the containers and volumes
```

`dev-smoke` prints `OK: request traversed client -> gateway -> signed policy ->
provider` when the whole chain works. It is idempotent, so `make dev-up` is safe
to re-run.

Two things that surprise people, both deliberate: the gateway is **not**
reachable from your host — it binds loopback inside a shared container network
namespace, exactly as it would inside an ECS task — and `model` must be the
literal string `"preferred"`, because the signed policy chooses the model rather
than the caller. See [local development](docs/LOCAL.md), which also covers going
from the mock to a real provider without a control plane.

## Who it is for

Four buyer personas, what each is trying to stop happening, and which deployment
shape each needs: [`docs/USE-CASES.md`](docs/USE-CASES.md). It includes the
argument for buying rather than building, which is made of specific failures
found here by measurement rather than of adjectives.

## What is implemented

- Loopback-only, single-tenant Go data plane; text chat normalization for OpenAI, Anthropic, Gemini and Amazon Bedrock. **All four stream.** Bedrock's AWS event-stream framing is translated to server-sent events at the provider boundary by `bedrockSSE`, so the generation deadline, finish tracking, empty-completion detection and the refusal to replay after acceptance apply to it identically rather than gaining an exception.
- Postgres persistence, migrations, hashed bearer credentials, tenant-scoped RBAC, row-level security, revocation and audit records.
- Ed25519 policy signatures, restricted canonical JSON, pinned overlapping verification keys, expiry, version rollback/equivocation protection and atomic disk cache.
- Inference uses only local policy and direct provider connections. Control-plane polling and telemetry delivery are background work.
- Bounded concurrency, token-bucket rate limiting, retry budget, circuit breakers, body/event limits, total generation deadline, slow-client protection and graceful shutdown.
- Nonblocking telemetry admission, fsynced disk spool, acknowledgement-based deletion, replay after process restart and deduplicated ingestion.
- OTLP/HTTP JSON trace export on an independent bounded queue, Prometheus counters/gauges, generated request IDs, W3C trace context and structured logs.
- Hardened Dockerfiles, ECS task example, Terraform sidecar/control-plane/RDS modules, CI and unit/integration scenarios.

## Supported API

```http
POST /v1/chat/completions
Authorization: Bearer <local application token>
Content-Type: application/json

{"model":"preferred","stream":true,"max_tokens":1024,"messages":[{"role":"user","content":"Hello"}]}
```

Only `model`, `messages`, `stream`, `max_tokens`, and `temperature` are supported. Text-only `system` (first message only), `user`, and `assistant` roles; final message must be `user`. Temperature is limited to the portable 0–1 subset. There must be one to 128 messages. Models are selected by the signed routing policy, not user-supplied provider model names.

**Tools, tool results, structured output, multimodal content, Responses API and provider-specific options are rejected.** Unknown request fields fail validation instead of being silently dropped. Set SDK retries to zero; an ambiguous failure is not an invitation to replay a generation. See [API and failover](docs/API.md).

## Build and test

Go 1.27 (or newer supported Go release) and Python 3.14:

```sh
python -m venv .venv
.venv/bin/pip install -r controlplane/requirements-dev.txt
make test          # go test -race ./... and the Python suite
make build         # writes bin/gateway
```

`make test` is the single source of truth for how the suites are run; invoke the
tools directly only if you need to narrow a run.

The Postgres tests need a **disposable dedicated cluster** and `TEST_DATABASE_URL`; they create schema and roles. CI supplies Postgres automatically and runs these tests. `SWITCHBOARD_IN_MEMORY_TESTS=1 go test -race ./...` uses an in-process HTTP transport when sockets are unavailable; this does not validate real network behavior.

## Deploy

Follow [deployment](docs/DEPLOYMENT.md), then [security and rotation](docs/SECURITY.md). No real credentials, private signing keys or Terraform state belong in this repository. The `.env.example` names the variables and where each value comes from; every value in it is empty.

Directories: `cmd/gateway`, `internal/gateway`, `controlplane`, `deploy`, `scripts`, `testdata`, and `docs`. The fixture public key in `testdata` is intentionally test-only and is not a deployment trust key.

## License

Open source under the [Apache License 2.0](LICENSE).

The auditability argument rests on the same ground it always did: every line is
readable, and the dependency graph is 4 direct and 12 indirect modules, all
AWS-published.

Contributions are not currently accepted. When that changes,
[CONTRIBUTING.md](CONTRIBUTING.md) sets the terms: sign-off under the
[DCO](DCO), plus a grant permitting relicensing and use in Switchboard
Recordkeeper, which is closed source. Contributors keep their copyright.
