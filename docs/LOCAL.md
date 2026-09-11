# Running Switchboard locally

A local stack that serves a real request end to end, with no AWS account, no
provider credentials and no per-token cost.

```sh
make dev-up      # bring the stack up and provision it
make dev-smoke   # send a real completion through it
make dev-down    # tear down containers and volumes
```

`make dev-up` is idempotent. Rerunning it reuses the keys and tokens already in
`.dev/env`.

## What it starts

| Service | Role |
|---|---|
| `postgres` | Stands in for RDS. The only service outside the shared namespace. |
| `taskns` | Owns the shared network namespace. Stands in for the ECS task. |
| `controlplane` | FastAPI control plane, signs and serves routing policy. |
| `mockprovider` | Local stand-in for OpenAI, Anthropic and Gemini. |
| `gateway` | The sidecar under test, on `127.0.0.1:8080`. |

**Why a shared network namespace.** The gateway refuses any listen address that
is not loopback, and refuses plain `http` to anything but `localhost`, so the
containers cannot address each other by compose service name. They share one
namespace and talk over `127.0.0.1` — which is also how this runs on ECS, where
every container in a task shares the task's namespace. The compose file models
production rather than working around it.

Two consequences worth knowing:

- The gateway is deliberately **not reachable from your host**. Anything that
  talks to it must run inside the namespace. `scripts/dev-smoke.sh` does this
  with `docker run --network container:switchboard-taskns-1`.
- `docker compose run` cannot attach to a service that uses
  `network_mode: service:`, so the provisioning and smoke jobs use `docker run`
  directly. This is a compose limitation, not a configuration mistake.

The control plane is published on the host at `http://localhost:18000` for
poking at directly.

## What `dev-up` does

The manual first-run sequence is long, and most of it is not automated anywhere
else in the repo. `scripts/devstack.py` collapses it into three steps:

1. **`keys`** — generates the Ed25519 signing keypair and every bearer token,
   writing them to `.dev/env`. The control plane gets the 32-byte seed as
   `POLICY_SIGNING_SEED`; the gateway gets the matching public key in
   `trusted_keys`. Nothing distributes these for you in production.
2. **`dbinit`** — applies migrations via `controlplane.migrate`, then creates
   `switchboard_rt`, a login role granted the `switchboard_app` role. Migrations
   run as the database owner; the runtime login is deliberately weaker, so row
   level security is actually exercised rather than bypassed.
3. **`init`** — provisions the tenant through `scripts/bootstrap.py`, registers
   an `agent` principal (the API stores only a SHA-256 of the token), publishes
   and signs policy version 1, and writes the gateway's `config.json`.

The gateway then polls the control plane every 15 seconds. `/readyz` stays 503
until the first signed policy verifies, so `dev-smoke` waits rather than races.

## Driving failure

`scripts/mockprovider.py` reads these environment variables, which make retry,
circuit-breaker and failover paths reproducible without touching a real provider:

| Variable | Effect |
|---|---|
| `MOCK_STATUS` | Return this status instead of a completion, e.g. `429`, `503` |
| `MOCK_FAIL_FIRST` | Fail this many requests, then succeed |
| `MOCK_DELAY_MS` | Sleep before responding, to drive timeouts and deadlines |
| `MOCK_TEXT` | Completion text to return |

Forcing `MOCK_STATUS=503` makes the gateway exhaust its retry budget and return
a visible `503 routes unavailable or retry budget exhausted`, which is the
behavior `docs/ARCHITECTURE.md` describes.

## Your first real provider request

The development stack above proves the plumbing against a mock. This is the
shortest path to a real completion from a real provider, and it needs **no
Postgres, no migrations, no database roles, no tenant, no principals and no
control plane**. The gateway restores a signed policy from `data_dir/policy.json`
at startup, and its readiness consults only that local copy.

The control plane is what you graduate to for rotation, multiple tenants and
policy distribution. It is not needed to see the thing work.

### 1. A signing key

The policy must be signed, and the gateway pins the public half.

```sh
python -c 'import base64,os; print(base64.b64encode(os.urandom(32)).decode())' > seed.b64
./bin/gateway -public-key < seed.b64
```

The seed is read on stdin rather than as an argument so it does not reach the
process table or your shell history. Keep `seed.b64`; you need it in step 3 and
for every later policy change.

### 2. Configuration

**Copy `config.example.json`; do not retype the block below.** It is elided —
the numeric limits are all required, and typing only what is shown produces a
run of validation failures, one per re-run.

Change three things: put the public key from step 1 in `trusted_keys`, set
`tenant` to whatever you like, and point `data_dir` somewhere that exists. The
example ships `/data`, which is right inside a container and not writable on a
laptop; use something like `./data` locally, and create it first.

The example's `trusted_keys` value is a real, well-formed key so that the file
starts as shipped — but it is **not yours**, and no seed in this repository
produces it. Leave it in place and the gateway will start, then refuse your
policy with `invalid signature`, which is now logged with the reason rather than
counted in silence. Replace it with your own public key from step 1.

```jsonc
{
  "listen": "127.0.0.1:8080",
  "tenant": "acme-prod",
  "data_dir": "./data",               // the example ships /data, for containers
  "control_url": "",                  // empty means file-only, no control plane
  "control_token_env": "",
  "local_token_env": "LOCAL_TOKEN",
  "trusted_keys": { "key-2026-09": "<the public key from step 1>" },
  "providers": {
    "openai": { "url": "https://api.openai.com", "key_env": "OPENAI_API_KEY" }
  }
  // the numeric limits follow; they are all required and each names itself if wrong
}
```

Two rules that cost people time:

- **Every configured provider needs its key variable set**, even one the policy
  never routes to. Configure only the providers you have keys for.
- `LOCAL_TOKEN` is what your application sends to the gateway, and it must be at
  least 32 bytes. It is not a provider key.

### 3. A signed policy

```sh
mkdir -p ./data
SWITCHBOARD_POLICY_SEED="$(cat seed.b64)" \
  python -m controlplane.policytool \
    --tenant acme-prod --key-id key-2026-09 \
    --route openai:gpt-4o-mini \
    --out ./data/policy.json
```

`--out` must land inside `data_dir` and be named `policy.json`; that is the only
file the gateway reads at startup in file-only mode.

`--key-id` must match the key in `trusted_keys`, and `--tenant` must match
`tenant`. Repeat `--route` for failover order, first preferred, up to four with
no provider repeated. The tool sets `issued_at` and `expires_at` itself; a policy
lasts seven days at most, and an expired one takes readiness to 503.

Changing routes later means signing a **higher** `--version` than the one already
stored. A lower or equal version is refused as a rollback.

### 4. Run it

```sh
export OPENAI_API_KEY=sk-...
export LOCAL_TOKEN=$(python -c 'import base64,os;print(base64.b64encode(os.urandom(32)).decode())')
./bin/gateway -config config.json
```

Then send a request. Note `"model": "preferred"` — the gateway rejects any other
value, because the signed policy decides which model runs, not the caller:

```sh
curl -s http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer $LOCAL_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"model":"preferred","messages":[{"role":"user","content":"Reply with the single word: ok"}],"max_tokens":512}'
```

The response carries `X-Switchboard-Provider`, naming which provider answered,
and `X-Switchboard-Attempts`. If an earlier route failed over, that is where you
see it.

### If it does not work

Each heading below is the message the software actually prints, so it can be
searched for. (Three of them used to be paraphrases that appeared nowhere in the
code, which is the worst possible thing for a troubleshooting list to be.)

- **`trust key "..." is not valid base64; derive it with 'gateway -public-key'`**
  — the value in `trusted_keys` is not a base64 public key. Replace it with the
  output of `./bin/gateway -public-key < seed.b64`.
- **`trust key "..." decodes to N bytes; an ed25519 public key is 32`** — right
  encoding, wrong value. You have probably pasted the seed rather than the
  public key.
- **`untrusted signing key`** — `--key-id` and the key name in `trusted_keys`
  disagree. The key itself may be fine.
- **`invalid signature`** — the key id matched but the key does not correspond to
  the seed that signed the policy. This is the one to expect if you copied
  `config.example.json` and did not replace the trust key.
- **`file-only operation, but no policy is present`** — nothing at
  `data_dir/policy.json`. Step 3 writes it; check `--out` matches `data_dir`.
- **`no valid routing policy or draining`** (503 from a request) — usually an
  expired policy. Sign a new one with a higher `--version`.
- **`/readyz` returns 503** — the body names the cause: no policy yet, the cached
  policy expired, a policy naming no configured provider, or draining. In
  control-plane mode `policy sync failed` is also logged once, with the reason
  and a hint, when the failure starts and again when it clears.
- **Anything about a numeric limit** — the error names the field and its range.
  If you typed the abbreviated config block rather than copying
  `config.example.json`, expect several of these in sequence: the numeric limits
  are all required.

### Graduating to the control plane

Set `control_url` and `control_token_env`, and the gateway polls for policy every
15 seconds instead of reading the file. That is when Postgres, migrations, roles
and principals become necessary, and `make dev-up` provisions all of it.

## Secrets

`.dev/env` holds a private signing seed and live bearer tokens in plaintext. It
is gitignored and must stay local. `devstack.py` prints tokens to stdout by
design, which is exactly what a deployment must never do — this tooling is for
local use only.

## Replaying a request

Every event carries the policy version that routed it, and the control plane keeps every signed
policy envelope forever, so proving where a request went is a join across two tables in the same
database:

```sh
DATABASE_URL=... python -m controlplane.replay \
  --request-id <the X-Request-ID the gateway returned> --tenant dev-tenant
```

It prints the policy that was live, its route order with the answering route marked, and whether
that provider was actually in the policy. A provider outside the route list means the policy rotated
mid-flight or something is wrong, and the report says so rather than staying quiet.

Two things to know when it finds nothing. Telemetry is delivered asynchronously and dropped rather
than retried forever, so a very recent request may not have arrived yet. And row-level security is
forced on `telemetry`, so the query sets `app.tenant` exactly as the control plane does; connecting
without it returns no rows rather than an error.

### Including the prompt and completion

Set `CAPTURE_TTL_SECONDS` in `.dev/env` and bring the stack up again. The gateway then writes each
request's prompt and completion to `<data_dir>/capture`, and `--capture-dir` includes them in the
report. It is **off by default here for the same reason it is off in the product**: this is the only
thing that writes prompts to a disk, and it should be chosen rather than inherited. The gateway logs
a warning at every startup while it is on.

Capture records never leave the machine — not to telemetry, not to the control plane — so reading
them needs access to the gateway's data directory. See `docs/SECURITY.md`.

## Sending telemetry to Honeycomb

OTLP export is off unless you turn it on. `devstack.py` reads `OTLP_URL`,
`OTLP_METRICS_URL` and `OTLP_HEADERS` from `.dev/env` and defaults them empty, because a dev stack
that silently shipped traces to somebody's account would be a surprise. To point it at Honeycomb,
add to `.dev/env`:

```sh
OTLP_URL=https://api.honeycomb.io/v1/traces
OTLP_METRICS_URL=https://api.honeycomb.io/v1/metrics
OTLP_HEADERS={"x-honeycomb-team":"HONEYCOMB_API_KEY"}
HONEYCOMB_API_KEY=<an ingest key>
```

`otlp_headers` maps a header name to the **name of an environment variable**, not to a value, so no
credential is written into the gateway's config file.

### Two keys, and they must not be the same one

| Variable | Kind | Used by |
|---|---|---|
| `HONEYCOMB_API_KEY` | Ingest | The gateway, to authenticate its OTLP export |
| `HONEYCOMB_CONFIG_KEY` | Configuration | `controlplane/honeycomb.py` only, never the gateway |

`.dev/env` is passed to the gateway container wholesale as `env_file`, so anything in it is readable
by the data plane. An ingest key there is correct: it can send telemetry and nothing else. A
**configuration** key there would let the gateway rewrite or delete the alerting that watches it,
which is the same mistake as letting it sign the policy it enforces — and the reason `policytool`
is not part of the gateway binary either.

`controlplane/honeycomb.py` therefore reads `HONEYCOMB_CONFIG_KEY` and will not fall back to the other variable.
Export it in your shell when you run the tool rather than adding it to `.dev/env`.

Honeycomb mints both kinds in its UI under **Environment settings > API keys**; nothing here creates
them, because creating a key through the API needs a more privileged key first, which only moves the
problem. What the tool does instead is check: it calls `/1/auth` before writing anything and reports
the team, the environment and any missing permission by name. A configuration key needs **Manage
Triggers, Manage Boards, Manage Recipients and Run Queries** — new keys do not have all four by
default.

Both the gateway and this tooling reach any OTLP backend. Grafana Cloud takes Basic auth, Datadog
takes `dd-api-key`, a local collector takes no header at all; only `controlplane/honeycomb.py` knows what
Honeycomb is.

## Showing a served-model change

`make dev-traffic` drives real requests through the gateway under a sequence of signed policies, so
`/dashboard` has more than one route in it. Its last phase is the one routing cannot explain: the
policy stays fixed and, midway, the mock provider starts answering openai as
`gpt-4o-mini-simulated-snapshot`. Same route, same provider, and a different model saying it served
the request, which is what a provider moving an alias to a new snapshot looks like.

Nothing is written behind the telemetry's back. The mock really returns a different model, through
`POST /control`, which exists only because `MOCK_CONTROL=1` is set on the `mockprovider` service. The
change is labelled before it is made:

- an annotation on the control plane, through `POST /v1/annotations`, which exists only with
  `SWITCHBOARD_DEV=1`. This is what `/dashboard/drift` reads to mark the change **simulated**.
- a Honeycomb marker of type `simulated`, when `HONEYCOMB_CONFIG_KEY` is exported in the shell you run
  it from. The key then also needs the Markers permission. `dev-traffic.sh` passes the variable by
  name, so it never enters `.dev/env`.

If the annotation cannot be written, the mock is not switched, so no chart shows an injected change
as an observed one. The phase then checks its own work -- the route header before and after, and
`/v1/series` showing the new model under a single policy version -- and exits non-zero if either is
wrong.

Afterwards `/dashboard/drift` shows the change. With OTLP export on, re-run
`python -m controlplane.honeycomb` once the `switchboard-gateway` dataset has a `gen_ai.response.model`
column, and the board gains the served-model panels; before that the tool lists them as left off.

## Verifying against real providers

The local stack runs against a mock, which is what makes it free and
deterministic. A mock cannot tell you whether the adapters match reality: it
only returns the fields they were written to expect.

That is not hypothetical. The first real call to Bedrock immediately found a
defect that would have failed every genuine request — the response carried a
field the wire struct did not model, and decoding was strict. The mock returned
only modelled fields, so it passed.

`internal/gateway/live_test.go` calls the real APIs. It is skipped unless
enabled, because it costs money and needs credentials:

```sh
SWITCHBOARD_LIVE_OPENAI=1    OPENAI_API_KEY=...    go test -run Live ./internal/gateway/
SWITCHBOARD_LIVE_ANTHROPIC=1 ANTHROPIC_API_KEY=... go test -run Live ./internal/gateway/
SWITCHBOARD_LIVE_GEMINI=1    GEMINI_API_KEY=...    go test -run Live ./internal/gateway/
SWITCHBOARD_BEDROCK_LIVE=1                          go test -run Bedrock ./internal/gateway/
```

Bedrock needs no key — it authenticates with the ambient AWS credentials.

Each request goes through `upstream()` and each response through `normalize()`,
which is the path a production request takes, so request construction,
authentication, response parsing, finish-reason mapping and token accounting are
exercised together. Three cases are covered per provider: a complete response, a
streamed response, and a deliberately truncated one, since an unmapped finish
reason is a hard failure rather than a degraded result.

Four provider cases run, not three. OpenAI's legacy and reasoning models take
different request shapes — reasoning models reject `max_tokens` outright — and
signal truncation differently, so `gpt-4o-mini` and `gpt-5-nano` are exercised
separately through the same adapter.

**The model names in `liveProviders` are a live dependency, not a constant.**
Two of the three originally targeted models were retired out from under these
tests and began returning 404. When a case fails with "no longer available", list
what the key can actually reach and update the table:

```sh
curl -s https://api.openai.com/v1/models -H "Authorization: Bearer $OPENAI_API_KEY" | jq -r '.data[].id'
curl -s 'https://api.anthropic.com/v1/models?limit=100' -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H 'anthropic-version: 2023-06-01' | jq -r '.data[].id'
curl -s 'https://generativelanguage.googleapis.com/v1beta/models?pageSize=200' \
  -H "x-goog-api-key: $GEMINI_API_KEY" | jq -r '.models[].name'
```

A provider that answers 503 or 429 is retried three times with backoff and then
**skipped**, not failed: that records "not verified", which is honest, where a
failure would wrongly accuse the adapter. Gemini hits this most often.

Cost is a few cents. Keys can be revoked afterwards, and should be.

### The startup provider check

Once the first signed policy verifies, the gateway sends one 8-token completion to each provider the
policy routes to, using a model that policy names. A failure withholds that provider from routing for
60 seconds and logs at ERROR. Routing recovers on its own through the breaker's half-open probe once
the account is funded or the key replaced, and the first request that then succeeds through that
provider also clears it from the readiness check, so neither requires a restart.

Setting `"provider_check_strict": true` additionally holds `/readyz` at 503, so the platform's own
health check replaces the task rather than letting it serve requests that will all fail upstream.
Default is `false`, because a gateway whose purpose is to keep serving when one provider is
unavailable should not refuse to start over one.

It sends a real completion rather than hitting an auth-only endpoint deliberately. `GET /v1/models`
returned 200 on an OpenAI key whose account had no credits, minutes before a completion on the same
key returned 429. A check that passes while every real request fails is worse than no check, because
it turns an obvious failure into a confident one.

### When a model returns nothing

Reasoning models spend their token budget on hidden reasoning before writing any answer, and reserve
nothing for the answer itself. If the budget runs out first, the provider returns HTTP 200 with
`finish_reason: length` and empty content, and bills for every token. Measured on `gpt-5-nano`:

```
max_tokens 1024   0 characters      1024 reasoning tokens   billed 1024
max_tokens 2048   1469 characters    832 reasoning tokens   billed 1159
```

The gateway fails over to the next route rather than returning an empty answer, and counts
`switchboard_empty_completion_total`. If every route does the same, the caller gets a 503 saying so
rather than a generic routing failure, because the fix is theirs: raise `max_tokens`.

There is no budget that avoids this generally. The same model answered "reply with ok" using 64
reasoning tokens, so the requirement is prompt-dependent. Watch the ratio of reasoning tokens to the
caller's budget: it approaches 1.0 before the output goes empty.

### Which key is actually in use

The gateway reads each provider's key from the environment variable named by `key_env`, and
`Config.Validate()` only checks that the variable is **non-empty**. Nothing distinguishes a present
key from a correct one, so a stale export in `~/.zshrc` or `~/.zshenv` silently outranks whatever you
meant to use, and the failure arrives later as a provider `401`, or a `429` mentioning credits. Both
read as a provider fault when they are a configuration one.

This is not hypothetical: two different OpenAI keys for the same account were in play during the live
verification above, one of which reported having no credits.

Compare what the shell has against what you think you are using, without printing either:

```sh
printf %s "$OPENAI_API_KEY" | shasum -a 256 | cut -c1-12
printf %s "$(cat ~/.switchboard/openai)" | shasum -a 256 | cut -c1-12
```

Matching digests mean they agree. Note that a non-interactive shell does not source `~/.zshrc`, so an
automated run and your terminal can legitimately disagree about the same variable.

For OpenAI specifically, the response headers name the account the key belongs to, which settles
"is this the account holding the credits" without guessing:

```sh
curl -s -D - -o /dev/null https://api.openai.com/v1/models \
  -H "Authorization: Bearer $OPENAI_API_KEY" | grep -i '^openai-organization'
```

A `user-` prefix is a personal account with no organization attached, so organization mismatch
errors do not apply to it. The gateway never sends an `OpenAI-Organization` header; `sk-proj-` keys
carry their own organization and project.
