# Status and remaining gaps

Last revised 2026-09-07, from evidence produced that day. Items move out of
"open" only when a run demonstrates them, never because the implementation looks
correct.

The Switchboard Sidecar is the deployable data-plane component of Switchboard.
This file covers the sidecar and the control plane it talks to.

## Verified

Each of these is backed by a run recorded in `docs/VALIDATION.md`.

1. **End-to-end request path.** A request traverses client, gateway, signed
   policy verification and provider, streaming and nonstreaming, over real
   sockets. Telemetry reaches Postgres through the disk spool and is
   acknowledged.
2. **Control-plane API, migrations, RLS, revocation and deduplication.** Run in
   CI against a real `postgres:17`, and again locally through the development
   stack, with migrations applied by the owner and the runtime connecting as a
   separate weaker login.
3. **Container builds.** Both images build and run; the control-plane image
   imports the application; the gateway serves.
4. **Terraform syntax and provider validation** across all three roots.
5. **CloudFormation templates** pass `cfn-lint` and server-side
   `aws cloudformation validate-template`.
6. **Sustained load.** 2,399 requests over 60s at 40/s, 8 workers, half
   streaming: 2,398 succeeded, p50 1 ms, p95 43 ms, p99 45 ms.
7. **Bounded backpressure.** 874,504 requests offered in 30s (29,149/s) against
   a configured limit of 50/s: 1,555 admitted, 872,888 rejected with 429, and
   p99 held at 10 ms. The gateway sheds load rather than queueing it.
8. **Circuit breaker and recovery.** With a provider failing, the breaker opens
   and rejects in about 1 ms without contacting the provider, then recovers
   unaided once the provider is healthy. Recovery timing is explained in item 13.
9. **Graceful shutdown under load.** SIGTERM during 3-second generations: the
   process exited after 4s with status 0, and every completed request shows a
   full generation latency, so in-flight work finished rather than being cut off.
10. **Python dependency reproducibility.** The full graph is pinned; a clean
    install resolves to exactly the 27 pinned versions.
11. **Go vulnerability scanning.** `govulncheck` reports no vulnerabilities. The
    toolchain is now aligned at Go 1.27 across `go.mod`, CI and the Dockerfile,
    and the `go` directive was raised so a stale toolchain cannot silently build
    the binary against a vulnerable standard library.
12. **Container image vulnerabilities — resolved.** The control-plane image
    previously reported 19 findings, 4 critical, all in Debian packages with no
    fix available upstream. Rebasing onto distroless removed the packages rather
    than waiting for fixes that do not exist: **the image now reports zero
    findings**, and is 7 MB smaller. perl and util-linux, which accounted for 13
    of the 19 and were never used by the application, are simply no longer
    present.
13. **Rate limits no longer trip the circuit breaker.** A 429 and a 503 both fed
    the breaker, so three rate limits withheld a healthy provider for fifteen
    seconds. A 429 is now a cooldown: it fails over, honours any `Retry-After`
    the provider sent, and records no failure. Measured with identical injection
    and load, five 429s cost five failed requests where they previously cost
    about 1,803 over 45 seconds; five 503s still cost 1,796, deliberately
    unchanged, which is the control showing the breaker was not weakened.
14. **Deployment, teardown and TLS.** The quickstart deploys, the bootstrap
    custom resource applies migrations and creates a runtime login distinct from
    the migration owner, and `/readyz` answers over TLS through the internal load
    balancer on a real certificate. Teardown removes everything billable and is
    guarded against running before a stack has finished deleting.
15. **Circuit-breaker recovery — explained, and it is not a defect.** The
    observed 45-second recovery is the breaker working as written: it trips at
    three failures, and each *failed* half-open probe re-arms a fresh full 15
    seconds. The provider under test failed five times, so three cycles elapsed
    before a probe succeeded. Confirmed by prediction: repeating the run with
    exactly three failures recovered in one cycle, 603 rejections against 1,795
    successes, where five failures gave 1,803 against 595. The code is
    self-consistent; `docs/ARCHITECTURE.md` was imprecise and has been corrected.

16. **Account-level provider failures now fail over.** Three real refusals were
    measured and are now classified from the response body rather than the
    status code alone: OpenAI out of credits (429 with no `Retry-After` and no
    rate-limit headers), Anthropic balance too low (400, not 402 or 429), and
    Gemini quota exhausted (429). Each withholds that provider for 60 seconds
    and tries the next route, counted as `switchboard_account_failover_total`.
    Classification fails safe: an unrecognised 400 stays terminal, because a
    genuinely malformed request would be refused identically everywhere and
    replaying it would multiply the waste rather than avoid it. A terminal
    rejection now carries the provider's own reason instead of a bare
    "provider rejected request".
17. **Providers are checked at startup.** Once the first signed policy verifies,
    each provider it routes to receives one real 8-token completion using a model
    the policy names. A failure withholds that provider and logs at ERROR;
    `provider_check_strict` additionally holds `/readyz` at 503 so the platform
    replaces the task. `/readyz` and the serving path are deliberately separate:
    the gateway keeps serving through a failed check, because refusing every
    request would deadlock recovery, since a successful request is what clears
    the check. A completion is used rather than an auth-only endpoint
    because `GET /v1/models` returned 200 on a key whose account had no credits,
    minutes before a completion on the same key returned 429.
18. **Telemetry delivery is batched.** Delivery was one POST per event, issued
    sequentially, so its real ceiling was round-trip bound rather than the
    hundred per tick the cap suggested: comfortable against a control plane in
    the same task, roughly twenty per second across a network at 50 ms. A spool
    that cannot drain grows to its cap and then drops events, and those events
    are billing and audit records.

    Measured after the change, at 150 requests per second for 45 seconds: 6,746
    events delivered in **47 requests**, 143.5 events each, with the spool
    empty and zero drops and zero export errors afterwards.

    The control-plane route is additive and the gateway falls back to the
    per-event route on a 404, or on a 200 that does not carry the expected
    acknowledgement, so either component can be deployed first. That is what
    makes this safe to ship while the `Event` token fields are not: those would
    be rejected by an older control plane's `extra="forbid"`.

    An event the control plane refuses is dropped and counted rather than
    retried forever, because a deterministic rejection resent every tick would
    wedge every event queued behind it.

    **Write churn was deliberately not fixed.** `atomicFile` costs three dentry
    operations per event, which is most of the reclaimable slab growth measured
    on 2026-09-08. Fixing it means appending to rotating segment files, which
    widens the crash-loss window from the in-memory queue to the in-memory queue
    plus the unflushed tail of the open segment. Widening a data-loss window on
    billing records to make a container memory graph look tidier is the wrong
    trade, and the slab growth is reclaimable rather than a leak.

19. **Two live routing defects, found while scoping something else, now fixed.**
    Both were reachable on the ordinary serving path with no feature flags, and
    neither was findable by tooling: `go vet`, `go build` and `go test -race`
    were clean before and after.

    **The circuit breaker was reset by provider failures.** A terminal fault
    called `circuit.result(false)`, which is the *success* path -- it zeroes
    `failures` and clears the open-until deadline. `classify()` returns
    `faultTerminal` for everything that is not 503/429/400/422, so 401, 403,
    404, 500, 502 and 504 all landed there. A provider returning 500 forever
    therefore reset its own breaker on every request and the breaker could never
    open. Now a 5xx counts as a failure and a 400/422 is neutral (`release()`),
    because a malformed request is the caller's fault and must not wipe out
    failures a sick provider has accumulated.

    **401, 403 and 404 now fail over.** They are refused before generation, so
    nothing was accepted and nothing was billed -- the argument `temperature.go`
    already makes for a 400 -- and they are specific to one provider, so the old
    comment's premise, "this request would fail the same way at every provider",
    was false for them. A rotated key or a retired model name meant 100% of
    requests returned 502 while a healthy provider sat idle in the same signed
    policy; item 2 records two of three model names in this repository's own
    tests being retired mid-project, so the 404 was not hypothetical. 500, 502
    and 504 remain terminal: the provider may have accepted the request and
    failed partway through generating it. New class `faultRefused`, new counter
    `switchboard_refused_failover_total`, and a 60-second cooldown so a rotated
    key does not cost a doomed round trip on every request.

    **Budget-aware routing was miscalibrated in both directions.** The streaming
    path recorded `producedText` as `text != ""` and the non-streaming path
    recorded `true` unconditionally. A response can legitimately carry no text --
    a Gemini prompt block, any `content_filter` finish, a `stop` with nothing to
    say -- so one content-filtered stream set `emptyAt` and shadowed a *healthy*
    model for the full six-hour TTL, and since `ParseChat` defaults `max_tokens`
    to 1024 that is most default traffic; while one content-filtered
    non-streamed response set `okAt` and silently disabled budget-aware routing
    for that model instead. Both now use `observeOutcome`, which records a fact
    only when the outcome is one -- `finish == "length"` with empty text, or text
    produced -- and stays silent otherwise. The predicate the empty-completion
    paths a few lines away already used.

20. **Diagnosability, and the first-run path.** `syncOnce` discarded eight
    distinct named errors from `Policies.Apply` in favour of one counter and no
    log line, so a wrong trust key, an unregistered control token, a tenant with
    no published policy and an unreachable control plane were indistinguishable:
    an empty-bodied 503 from `/readyz`, forever, in silence. It now names which
    failure occurred with a hint at the thing to check, logged when the failure
    starts and again when it clears rather than every fifteen seconds; `/readyz`
    and `/runtime` carry the reason; four `fatal` paths in `cmd/gateway/main.go`
    that dropped their error now pass it; and the control plane logs the
    exception type and traceback instead of a bare request id, and reports an
    unhandled fault as 500 rather than as a retryable 503.

    The correction to item 17's note: the fifteen-second poll was **not** why a
    first control-plane attempt looked like a hang. `Sync` calls `syncOnce`
    before waiting on the ticker, so the first poll is immediate and a healthy
    start reaches `/readyz` 200 in well under a second. The fifteen seconds only
    bit after a failure, and the defect was that the failure was invisible.

    Also: the latency histogram covered two populations, since `ObserveLatency`
    ran in the `defer` for every request including 401s and policy refusals that
    complete in microseconds. `latencyBounds` starts at 5 with no lower edge on
    the first bucket, so enough of them interpolated the median below zero --
    the `P50 = -5 ms` in item 5. It now measures only requests that reached a
    provider.

    `make dev-up` and `make dev-smoke` were verified from a clean state after
    these changes: 37 seconds to a provisioned stack, then `OK: request
    traversed client -> gateway -> signed policy -> provider`. A `devstack` job
    in `.github/workflows/ci.yml` now runs both on every push, which nothing did
    before -- the one path a newcomer takes was the least protected thing in the
    repository, and it can only rot silently, because whoever would notice
    already has warm images and a populated `.dev/env`.

21. **The local stack only ran in a directory called `switchboard`, and the CI
    job added to protect it found that on its first run.**

    Docker Compose derives its project name from the working directory and every
    container name follows. `scripts/dev-up.sh`, `scripts/dev-smoke.sh` and
    `docs/LOCAL.md` all name the namespace container directly -- they have to,
    because `docker compose run` cannot attach to a service using
    `network_mode: service:` -- so any checkout not named exactly `switchboard`
    produced `<dir>-taskns-1` and every reference missed with
    `No such container: switchboard-taskns-1`.

    `git clone` from the sidecar remote produces `switchboard-sidecar/`, so the
    documented first command did not work for anyone who cloned that repository
    normally. It went unnoticed for the reason these things always do: the person
    most likely to run it had cloned into a directory that happened to match.

    The evidence is unusually clean. The same two commits passed `verify` on
    `Grace/switchboard` and failed it on `Grace/switchboard-sidecar` -- identical
    SHAs, different checkout directory, and `actions/checkout` names the directory
    after the repository. Fixed by pinning `name: switchboard` in
    `docker-compose.dev.yml`, which makes the project name independent of where
    the repository sits and leaves all three existing references correct.

    Verified by reproducing the CI condition rather than reasoning about it: a
    worktree in a directory literally named `switchboard-sidecar` failed exactly
    as CI did before the change and ran clean through `make dev-smoke` after it,
    and the original directory still does both.

22. **A policy expires within seven days and nothing said so until it had.**

    `controlplane/policy.py` refuses a lifetime over 604800 seconds and the
    gateway's `verify()` enforces the same bound. The cap is deliberate and
    right: it bounds what a compromised signing key is worth. Renewal is manual,
    and `docs/SECURITY.md` says so.

    What was missing is that **nothing measured the remaining time.** No metric,
    no alert, no log line. `/runtime` carried `policy_expires_at` behind the
    local bearer token, and nothing polls it. So the first signal a deployment
    received was `/readyz` turning 503, after it had already stopped serving. A
    system that worked for a week and then stopped, with no indication a clock
    had been running, is a worse experience than one that never started, because
    by then the operator believed it.

    Now `switchboard_policy_expires_in_seconds`, a gauge on the same sampled
    path as the runtime gauges so both emitters get it from the one `series()`
    list, negative once expired. Plus a warning at 48 hours and an error at
    expiry, logged on band change rather than per tick — two days of one-minute
    ticks is 2,880 identical lines, which buries the one it is raising. Both
    modes start the watcher: file-only operation is the one with no control
    plane to notice on the operator's behalf.

    **Not alerted on, for a reason worth writing down.** `controlplane/alerting.py`'s
    `Condition` is counter-only by construction — its own docstring says the
    question is always "did this go up during the window" — so a gauge threshold
    cannot be expressed there without extending the type and both emitters. And
    the Honeycomb free plan allows two triggers per team, which
    `controlplane/honeycomb.py` already spends by folding four conditions into
    two. A fifth condition that cannot fold into a counter formula has nowhere
    to go. The gauge and the log are what work today; the alerting layer needs a
    gauge-threshold shape before this can join it.

    **Automatic renewal is still not built**, and is a larger question than
    visibility: who re-signs, on what trigger, and what happens when the signing
    key is unavailable. Making the deadline visible was the part whose absence
    turned a documented limit into a surprise.

23. **The telemetry named two of four providers off-enum, and threw away every
    token count.** Switchboard emits the OpenTelemetry GenAI conventions
    natively rather than a dialect, so the normalizers that translate one into
    the other -- `genainormalizerprocessor` upstream, and `genai-interlingua` --
    do not apply to it. Checking that claim found four defects instead.

    **`gen_ai.provider.name` carried our own identifiers.** Two of the four also
    happen to be the convention's values; `gemini` and `bedrock` are not, and
    the enum says `gcp.gemini` and `aws.bedrock`. This fails silently: nothing
    rejects an off-enum value, so that traffic simply stopped grouping with
    everything else those providers serve, on the attribute the conventions name
    as the discriminator the rest of the span is read through. The identifiers
    cannot move -- `controlplane/app.py` validates them as a `Literal`, signed
    policies carry them, `policy.go` enforces them -- so the translation sits at
    the OTLP boundary. Gemini is `gcp.gemini` and not `gcp.vertex_ai` because the
    registry scopes that value to `generativelanguage.googleapis.com`, which is
    the endpoint `adapter.go` calls.

    **`gen_ai.operation.name` was absent**, and it is the one attribute marked
    Required on both the span and the token metric. It is per provider, not a
    constant: a well-known value MUST be used where one applies, so Gemini's
    `generateContent` is `generate_content` and the other three are `chat`,
    Bedrock's Converse included.

    **Token counts were computed for every provider and dropped.**
    `adapter.go` has always normalised four incompatible usage shapes into one
    set of numbers, including Anthropic's three-way cache split where reading
    `input_tokens` alone under-counts without bound. They fed a single integrity
    check and went nowhere else, so nothing downstream could say what a request
    cost. They now travel as `gen_ai.usage.*`, and the cache parts are kept
    beside the sum they belong to: the sum is the billing figure, the parts are
    what say whether caching is working, and a total alone cannot tell a warm
    prompt from a cold one. An output of zero is reported rather than suppressed,
    because a reasoning model spending its whole budget before writing a word is
    exactly what this gateway exists to notice.

    **Every metric was one undimensioned number.** No per-provider cost, latency
    or failure reading existed anywhere, and that -- not the name -- was what
    blocked the rename: `gen_ai.server.request.duration` requires
    `gen_ai.operation.name` and `gen_ai.provider.name`, and a conformant name
    over data missing the attributes it requires is worse than an obviously
    custom one. It is the `bedrock` defect again, moved to metrics. So the
    histogram is now keyed by provider, model and `error.type`, renamed, and
    converted to seconds at the OTLP boundary; `gen_ai.client.token.usage` rides
    the same keys. `switchboard.request_duration_milliseconds` is gone from OTLP
    and any board, query or trigger built on it has to be recreated.

    The route table is capped at 128 keys carrying a model, because models
    arrive from signed policies and that key space has no bound of its own.
    Past the cap an observation folds onto its provider rather than being
    dropped: losing a dimension beats losing the measurement, and a counter says
    the detail is missing rather than leaving a silent hole.

    **What did not change, and why.** `switchboard.attempts`, `switchboard.fault`
    and `switchboard.policy_version` stay where they are. `gen-ai-spans.md`,
    `gen-ai-agent-spans.md` and `gen-ai-metrics.md` were read directly: there is
    still no gateway, router, failover or retry convention, so item 18's
    reasoning holds.

    **`/metrics` was left in milliseconds for a day, on a reason that was
    wrong.** The argument was that OTel semconv governs OTLP, Prometheus
    exposition has its own rules, nothing scrapes that endpoint in a shipped
    deployment, and renaming it would break the tables in `docs/DEPLOYMENT.md`.
    That last clause is simply false: no document anywhere names a Prometheus
    metric, and those tables use the dotted OTLP names. The rest was true and
    not sufficient. One measurement carried two names, two units and two
    dimensionalities across two surfaces of the same product, which is the kind
    of inconsistency a customer reads as carelessness.

    Both surfaces now render the same keyed data. The Prometheus names are
    derivations rather than inventions: OpenTelemetry's Prometheus mapping
    replaces dots with underscores, converts the UCUM unit to a word and appends
    it, and drops bracketed units, giving
    `gen_ai_server_request_duration_seconds` and `gen_ai_client_token_usage`.
    Attributes become labels by the same rule. The `switchboard_*` counters keep
    their names, because nothing above covers routing.

    Aligning **removed** code rather than adding it. `LatencyBuckets` and
    `ObserveLatency` existed only to feed the Prometheus histogram, so the whole
    parallel path is gone and `Server.chat` makes one observation instead of
    two. The test that checked the two surfaces agreed went with it: they now
    cannot disagree, which is better than verifying that they had not.

    One trap runs in both directions and is worth naming. Prometheus buckets are
    cumulative, which is what is stored; OTLP wants them differenced. Using
    either form where the other belongs produces a histogram that looks
    plausible and is wrong everywhere except the first bucket, so the conversion
    lives in one function and both directions are asserted.

    **A defect of my own, found and fixed in the same pass.** The first version
    of `gen_ai.client.token.usage` was a degenerate histogram: a single bound
    with a correct sum and a meaningless shape. Cost queries would have been
    right and any percentile of tokens per request would have been wrong rather
    than absent, which is exactly the silent wrongness this item began by
    describing. It now has real bounds spanning the budgets `ParseChat` defaults
    to and the reasoning overruns item 3 records, observed per request.

## Still open

1. **Deployment is proven for the quickstart only.** `quickstart.yaml` has been
   deployed, verified and torn down in one account, in us-east-1, with one
   certificate, on Postgres 17.11. **`controlplane.yaml` has never been
   deployed**, no other region has been tried, and the Postgres 18.6 default now
   in the template has never been deployed either — that default and the derived
   parameter-group expression remain unexercised.
2. **Live provider traffic, now verified; two behaviours recorded.** All four
   adapters have been exercised against the real services, complete and
   streaming, and two defects were found and fixed (Gemini metered output tokens
   as zero; OpenAI reasoning models were unusable because the request sent
   `max_tokens`). See `docs/VALIDATION.md`. What remains open is narrower:
   regional availability and data-retention suitability are still unassessed;
   `gemini-3.6-flash` returns 503 under load and 429 when the suite is run in
   quick succession, so a run can need retries and will skip rather than fail
   when all of them are refused, meaning a green suite does not by itself prove
   Gemini was reached; and model names
   proved to be a live dependency rather than a constant, with two of the three
   originally targeted models retired out from under the tests.
3. **Budget-aware routing, on measured behaviour.** A route is skipped when that
   model was already seen returning no text at the caller's `max_tokens`, and has
   never been seen succeeding at or below it. The gateway used to discover this
   per request, pay for it, and fail over; it now avoids the call.

   Verified live against real providers with a policy of `openai:gpt-5-nano` then
   `anthropic:claude-haiku-4-5`: the first 1024-token request reported
   `X-Switchboard-Attempts: 2` after the reasoning model returned nothing, and an
   identical second request reported `1`, having skipped it. Both callers got a
   real answer of over 1,500 characters.

   Two facts per model, not a statistic: the largest budget seen producing
   nothing, and the smallest seen producing text. Averaging would be confidently
   wrong, because the same prompt consumed 1920 reasoning tokens at a budget of
   2048 and 1152 at 4096. A single success overrides any number of failures, so
   the rule corrects itself rather than latching, which bounds its known
   imprecision: demand depends on the prompt, so a hard prompt failing can
   briefly shadow an easy one at the same budget.

   **If every eligible route would be skipped, none is.** Refusing to try is
   worse than trying and failing over, and a policy whose routes are all
   reasoning models is exactly the case this exists for. Observations are bounded
   and expire after six hours, because model behaviour moves: two of the three
   models named in the live tests were retired by their providers mid-project.

   `ParseChat` still defaults `max_tokens` to 1024, and that is still the budget
   measured returning nothing for four of seven ordinary prompts on `gpt-5-nano`.
   Raising it remains ruled out on evidence: 4096 also returned nothing for a
   500-word essay prompt, while `gpt-4o-mini` answered it inside 666 billed
   tokens. Routing around the problem is the fix; a bigger number is not.

4. **Metrics only leave the task if you configure it.** `/metrics` is loopback
   only, and the scraping collector is an opt-in container absent from both
   CloudFormation templates and the sample task definition, so a default
   deployment exposes no counters at all. Setting `otlp_metrics_url` pushes every
   counter, gauge and the request-duration histogram over OTLP with no extra
   container. The gateway now warns at startup when neither path is configured,
   so the default silence is at least visible in the log stream.
5. **Alerting goes through Honeycomb. The export path is proven; the alerts on
   top of it are half-built.**
   Gateway alarms hang off metrics in Honeycomb rather than CloudWatch, which is
   now a decision rather than an omission. CloudWatch remains reachable by the
   same mechanism whenever it is wanted.

   The blocker was that OTLP export could not authenticate to anything: both
   exporters set only `Content-Type`, so `otlp_metrics_url` worked against an
   unauthenticated collector on loopback and nothing else. `otlp_headers` now maps
   a header name to the **name of an environment variable**, matching how every
   other secret in this configuration is handled, and both exporters apply it.
   Header names are validated as HTTP tokens, so a hand-edited config cannot
   inject CR or LF, and the map cannot override `Content-Type` or `Authorization`.

   **Verified against Honeycomb on 2026-09-08.** Pointing the dev stack at
   `api.honeycomb.io` produced two datasets in a test environment within a
   minute: `metrics`, carrying 28 `switchboard.*` columns,
   and `switchboard-gateway`, carrying spans with `gen_ai.provider.name`,
   `duration_ms` and `trace.trace_id`. The 28 are 20 counters, 7 gauges and the
   request-duration histogram, which is exactly the list `series()` builds in
   `internal/gateway/telemetry.go` — nothing is lost between the exporter and the
   backend. Both exporters authenticate through `otlp_headers`, so the path is no
   longer exercised only against an `httptest` server.

   Waiting for data was the right call, and it caught a real defect: **the two
   paths spell the counters differently.** `/metrics` emits
   `switchboard_empty_completion_failed_total`; OTLP emits
   `switchboard.empty_completion_failed_total`. Every trigger in
   `docs/DEPLOYMENT.md` had been written in the `/metrics` spelling, so all four
   would have referenced columns that do not exist. The table is corrected.

   **Two of the four triggers exist. Both were dead until 2026-09-08, and looked
   fine.** They were created aggregating with `RATE_SUM`, which Honeycomb does
   not permit on a Metrics dataset: running that query by hand returns
   `aggregate operation not allowed in Metrics dataset: RATE_SUM`. A trigger
   holding a query the engine refuses cannot evaluate, so both displayed as
   healthy and neither could ever have fired. Both now aggregate with `SUM`,
   which on a cumulative counter is the increase over the trigger window, and
   that query was run against the live dataset before the change was made rather
   than assumed. This is the same failure mode as the column-spelling defect
   above and it is worth stating plainly: **Honeycomb accepts a trigger it will
   not run, and says nothing afterwards.** The only reliable check is to execute
   the trigger's own query.

   **The free plan allows two triggers, and all four conditions fit anyway.** The
   API answers a third create with `exceeded maximum 2 triggers for this team's
   plan`. The MCP tooling in front of it reported only `Failed to save trigger`,
   which is what made this look like a query problem for as long as it did;
   calling the API directly gave the real message immediately. A trigger query
   may hold only one aggregate — a second is refused with `query: only one
   non-having aggregate is allowed` — but a **formula** reduces several to one
   value, and formulas do run on a Metrics dataset. So slot 2 now watches
   `account_failover + usage_mismatch + idempotent_unknown` and fires above zero,
   while slot 1 keeps the page for lost service to itself. The cost is that slot
   2 says something moved rather than which; the `Switchboard gateway` board
   answers that in one panel, and boards are not capped.

   **Both triggers now notify.** One email recipient, attached to both. They had
   none, which was worse than having no trigger because it read as coverage.

   **Latency percentiles are unusable, and the histogram is not at fault.**
   `P50`, `P95` and `P99` on `switchboard.request_duration_milliseconds` (since
   renamed to `gen_ai.server.request.duration`, in seconds -- see item 23) all
   return **-100 ms**. A duration cannot be negative, but the encoding is
   correct: `HISTOGRAM_COUNT` returns exactly `requests_total`, so every
   observation is accounted for. The cause is `latencyBounds` in
   `internal/gateway/telemetry.go`, which starts at 100 ms. Every request so far
   is faster than that, so all of them land in the first bucket — and the first
   bucket of an explicit-bounds histogram has no lower bound, so any percentile
   drawn from it is extrapolation into negative time. Honeycomb's own value axis
   confirms it: the range tops out at exactly 100, the first bound.

   **Fixed as far as it can be, which is not all the way.** `latencyBounds` now
   starts at 5 ms rather than 100, and `LatencyBuckets` is sized from
   `len(latencyBounds)` so the array and the bounds cannot drift apart. The
   change is purely additive: every previous bound survives, so a query written
   against `le="100"` or above still means what it did. Measured after the
   change, on traffic mixing successful mock completions with 401, 400 and 503
   rejections: **P95 and P99 came back at 62.5 ms, where every percentile used
   to be -100. P50 is still -5.**

   That residue is structural rather than a missed spot. The first bucket of an
   explicit-bounds histogram has no lower edge wherever the floor is put, so
   whatever fraction of traffic falls beneath the lowest bound always yields a
   negative estimate for that fraction. Here the sub-millisecond rejections are
   about half the requests, so the median still lands inside it. Lowering the
   floor again would move the problem rather than remove it.

   The cause underneath was that one histogram measured two populations:
   requests that reached a provider and took tens of milliseconds or more, and
   requests refused locally in microseconds. `ObserveLatency` was called from a
   `defer` in `Server.chat`, so every 401, rate-limit rejection, policy refusal
   and idempotent replay sat in there alongside the inference.

   **Since fixed, and this paragraph used to say it was not.** `ObserveLatency`
   now runs only when `event.Attempts > 0` -- incremented immediately before each
   upstream call, so it is exactly "did this request reach a provider". Item 20
   records the change. The two entries contradicted each other for as long as it
   took to notice, which is the failure mode this file exists to prevent, so it
   is worth saying plainly rather than quietly editing.

   **Both known causes are now gone, and the reading is still unverified.** The
   floor is 5 ms and the local rejections that made up about half the traffic are
   out of this population entirely, so the two mechanisms that produced -100 and
   -5 are both addressed. Nothing has been measured since: item 23 renamed the
   metric to `gen_ai.server.request.duration` in seconds and the stack was torn
   down before any traffic ran against it, so the P50 figure above is the last
   real observation and describes a histogram that no longer exists under that
   name. Whether percentiles now read correctly needs one deployment's worth of
   traffic to say, and until then the honest claim is that the causes were
   removed rather than that the numbers are trustworthy.

   The structural residue above still applies to whatever remains below the
   lowest bound, and the numbers quoted are from before the fix. They have not
   been re-measured against real traffic, because there is none. Read the tail
   and the heatmap for shape until a deployment produces enough requests to say
   whether the median now means something.

   **Traces carried a correct error signal that nothing could consume.** Spans
   set the OTLP status to `code: 2` on any 4xx or 5xx and always had, and it
   arrives in Honeycomb as `status_code`. Honeycomb's anomaly detection does not
   read it. Asked directly, the account named the fields it does read:
   `error`, `error.message`, `error.type`, `exception.message`, `exception.type`.
   The service was therefore reported as having no recognised error attributes
   and refused monitoring outright, which is a gap that looks like working
   telemetry from every angle except the one that matters.

   Failed spans now carry `error.type`, valued as the status code, which is what
   OTel's HTTP convention prescribes when there is no exception class and is also
   the honest taxonomy here: every `fail()` in `server.go` picks a distinct status
   for a distinct cause. `error.message` is deliberately not emitted, because some
   of those messages are derived from a provider response or from decoding the
   request body, and `docs/SECURITY.md` promises neither appears in product
   telemetry. One recognised attribute is enough to be monitored and is not worth
   a written guarantee. Confirmed against the live dataset: `error.type` appears,
   and Honeycomb derived a boolean `error` column from it unprompted, so two of
   the five fields are now populated.

   **The service still reads ineligible, and that is now a deployment property
   rather than an open defect.** Honeycomb re-evaluates eligibility weekly. The
   evaluation that produced "doesn't have any recognized error attributes" ran at
   `2026-09-08T08:00:04Z`; `error.type` first arrived at `17:35Z`, more than nine
   hours later. The banner therefore describes a state that no longer exists, and
   the API still reports that same `updated_at`. The next evaluation is a week
   out, so nothing observable can change before it, and the fix cannot be
   confirmed by watching the status flip.

   The coverage bar is the harder gate and it is quantified: the **presence**
   signal reports that detection **needs 85% data coverage and the service is at
   0%**. Coverage of a trace dataset means spans arriving in most evaluation
   windows, and spans are emitted per request, so a gateway that is running but
   idle produces none. A dev stack driven in bursts is structurally incapable of
   reaching 85%. Only a service that is deployed and actually serving satisfies
   it, which also means anomaly detection is not something a new deployment has
   on day one. Triggers are: they evaluate on their own schedule with no coverage
   requirement at all, which is the argument for having built them rather than
   waiting.

   **Spans now carry the routing decision.** `gen_ai.request.model`,
   `switchboard.attempts` and `switchboard.fault`, all read from values the
   routing loop already had and discarded. Before this the metrics could say how
   often failover happened while no trace could say what happened to one request.
   `switchboard.fault` is the interesting one: `fault.go` already reduces four
   providers' incompatible failure vocabularies (`ThrottlingException`, `429`,
   `RESOURCE_EXHAUSTED`, `overloaded_error`) to `terminal` / `rate_limit` /
   `account` / `degraded`, and that classification was being used for a routing
   decision and then thrown away. A span may show status 200 with a fault set:
   that is a request that succeeded by routing around a refusal, and reading it
   beside `attempts` is the point.

   Both new `Event` fields are `json:"-"`. The control plane declares its event
   model `extra="forbid"`, so one unrecognised key does not degrade an event, it
   rejects it, and every event would fail. Spans reach the OTLP endpoint without
   passing through the control plane, so this costs nothing. Verified after the
   change: control-plane telemetry still returns 200.

   The namespace is `switchboard.*` rather than `gen_ai.routing.*` deliberately.
   No OpenTelemetry convention covers a router yet, and an experimental namespace
   is what OpenTelemetry asks for while that is true. If these four fault
   categories are still the right four after real traffic across four providers,
   that is the piece with any claim on going upstream.

6. **Replay is bounded in ways worth knowing before relying on it.** Every
   event now carries `policy_version`, and the control plane keeps every signed
   envelope, so any request's routing decision is reconstructible with
   `controlplane/replay.py` - which policy was live, its route order, and whether
   the provider that answered was one that policy allowed. That part has no
   retention limit beyond telemetry's own.

   Content is different. `capture_ttl_seconds` is off by default, so **nothing
   before it was switched on can ever be replayed with its prompt**, and nothing
   past the TTL survives. Records are local to the gateway that served the
   request, so a fleet has no single place to look and a task that has been
   replaced has taken its captures with it. Telemetry is delivered asynchronously
   and dropped rather than retried forever, so a request whose event never
   arrived is unreplayable even if its capture is on disk.

   The deployment-ordering constraint is real and was observed rather than
   theorised: the control plane's event model forbids unknown fields, so a
   gateway sending `policy_version` to a control plane that predates it has every
   event rejected at per-element validation and dropped. **Deploy the control
   plane first.** `scripts/dev-up.sh` did not rebuild the control plane image at
   all, which is how this was found.

7. **Anthropic prompt-cache tokens, now counted.** Anthropic splits input three
   ways and `input_tokens` is only one of them: its documentation defines that
   field as "the tokens that come after the last cache breakpoint in your
   request, not all the input tokens you sent", and gives
   `total_input = cache_read + cache_creation + input`. The three are disjoint.

   Reading `input_tokens` alone was therefore not a rounding error but an
   unbounded under-count. Anthropic's own worked example is 100,000 tokens read
   from cache plus a 50-token message, which reports `input_tokens: 50`; the
   gateway would have told the caller that request cost 50 input tokens when it
   cost 100,050. All three are now summed.

   It was invisible rather than absent: `cache_control` is opt-in, so both fields
   are zero on every request made so far and the arithmetic was correct by
   accident. The defect would have arrived with the first cached prompt, not with
   a deployment. This does not touch AWS Marketplace billing, which meters per
   task-hour rather than per token; it is the `usage` block returned to callers,
   and anything built on it for cost attribution.
8. **Reasoning models constrain `temperature` -- since handled, and this entry
   was stale.** They reject any value other than the default. This said "not
   fixed blind" long after it was fixed: `internal/gateway/temperature.go` and
   the retry path in `server.go` now detect the refusal from the provider's own
   wording, resend once without the field, remember the model for six hours in a
   bounded table keyed `{provider, model}`, and count
   `switchboard_temperature_dropped_total` so the substitution is visible rather
   than silent. What remains genuinely open is narrower: the phrase list is
   lifted from real refusals and has no version to pin, so a provider rewording
   its message costs one failed request before the table relearns.
9. **Bedrock streaming, verified against the real service.** All four providers
   stream. Bedrock's AWS event-stream framing is translated into server-sent
   events at the provider boundary by `bedrockSSE`, so the generation deadline,
   finish tracking, empty-completion detection and the refusal to replay after
   acceptance apply to it identically rather than gaining an exception.

   The translator holds the stop reason back and emits it together with token
   usage, because Bedrock sends those as two separate events and the pipeline
   terminates on the first frame reporting completion. Without that, every
   streamed Bedrock request would have reported zero tokens. A live run confirms
   it: 17 frames, `finish=stop`, `in=5 out=47`, with usage intact.

   What remains unexercised on this path: tool and reasoning blocks are refused
   rather than flattened, and that refusal has only been tested with synthetic
   frames, because Nova Micro does not emit them for these prompts.
10. **Spool file churn grows reclaimable kernel slab. Not a leak, and the earlier
   claim that it was is retracted.** A 30 minute soak measured resident memory
   rising from 7.4 MiB to 26.9 MiB and this file previously called that the most
   important open item, projecting an out-of-memory kill within a day. That was
   wrong. It read `docker stats` without decomposing what the number contains.

   Measured with pprof and the container's own cgroup accounting:

   | | growing? |
   |---|---|
   | Go `Sys`, all memory the runtime holds | +0.25 MB in 3 minutes |
   | `HeapAlloc`, live objects | flat, slightly down |
   | `Mallocs` minus `Frees` | equals `HeapObjects` exactly |
   | goroutines | 34 then 24, down |
   | cgroup `anon`, the process | flat at 8.65 MB |
   | cgroup `slab_reclaimable` | **4.60 to 5.79 MB in 2 minutes** |
   | cgroup `slab_unreclaimable` | zero throughout |

   Resident growth tracks slab, and slab is entirely reclaimable. The telemetry
   spool writes one file per event and unlinks it after the control plane
   acknowledges, so 20 requests per second churns 40 dentry operations per
   second, and the kernel caches those dentries and inodes against this cgroup.
   At roughly 0.55 MB per minute that accounts for about 16.5 MB across 30
   minutes, which matches the 19.5 MB originally reported.

   The consequence is real but different: reclaimable slab counts toward a
   container memory limit, and the kernel frees it under pressure rather than
   killing the process. It will look like unbounded growth in any monitoring that
   watches container memory, which is worth documenting for operators. Batching
   the spool into segment files rather than one file per event would remove the
   churn, and is the fix if this ever needs one.
11. **Idempotency keys, bounded and off by default.** `Idempotency-Key` is
    honoured when `idempotency_ttl_seconds` is set. A duplicate key returns the
    original response without calling the provider; the same key with a different
    body is refused with 422; a key whose original outcome is genuinely unknown
    refuses the retry with 409 rather than risk charging twice.

    That last case is the point. Four paths in `chat()` end with generation
    ambiguous, and the entry is deliberately sticky there. An unambiguous failure
    such as a provider 400 releases the key instead, so a caller who can fix the
    request is not blocked by their own typo.

    **Exactly-once generation across provider APIs remains impossible**, and
    nothing here claims otherwise. What changed is the window in which the
    ambiguity costs money. Entries survive a process restart, because they are in
    `data_dir` under the existing exclusive lock, but **not task replacement**,
    since that directory is ephemeral unless mounted on EFS. A retry landing on a
    replacement task is a new request. Anything stronger needs durable shared
    storage, which is a different product decision.

    Off by default because entries hold request and response content, which is a
    new category of data at rest; `docs/SECURITY.md` says so plainly rather than
    leaving the earlier "not logged or in telemetry" statement to imply more than
    it covers.

12. **Security operations.** Bearer RBAC exists; SSO, MFA, human-user lifecycle,
   external authorization, hardware-backed signing and automated key renewal do
   not. Row level security defends against query mistakes, not against a
   compromised shared database session.
13. **Scale and operations.** Rate limits and circuit state are per process, not
   fleet-wide. There is no retention policy, partitioning, dashboard, SLO, audit
   export or restore drill.
14. **The release pipeline works, and the first OIDC run failed for a reason
    worth writing down.** `v0.4.0` published both images to ECR with cosign
    signatures on 2026-09-08, carrying 105 commits.

    The first attempt was refused with `Not authorized to perform
    sts:AssumeRoleWithWebIdentity`, and everything that error usually means was
    already correct: the role ARN, the `sts.amazonaws.com` audience on both the
    provider and the trust policy, and `id-token: write` on the job. It is also
    not propagation, though it looks exactly like it — `configure-aws-credentials`
    retried 13 times across 84 seconds and was denied identically each time.

    GitHub issues an **ID-qualified subject**. The repository's own API returns
    `"sub_claim_prefix": "repo:Grace@14958021/switchboard@1356690158"`, so the
    token carries that prefix and the trust policy was matching
    `repo:OWNER/REPO`. The suffixes are rename resistance: the user and
    repository IDs are stable even if either name changes.

    `github-oidc.yaml` now takes a `SubjectPrefix` parameter, defaulting to the
    classic form so an existing stack is unaffected. A parameter rather than a
    wildcard on purpose: `repo:owner*/repo*` would match this token and also
    anyone registering a similarly-named account, trading a correctness bug for
    a privilege escalation.

15. **A release pipeline has run three times before the rewrite.**
    An earlier heading here said the pipeline had never run, and that was wrong
    in the direction that matters: it understated what already ships.

    Three `release` runs completed successfully, roughly four minutes each, on
    tags this repository still carries:

    | tag | run | date |
    | --- | --- | --- |
    | `v0.1.0` | 33892592190 | 2026-09-04 |
    | `v0.2.0` | 33937475292 | 2026-09-05 |
    | `v0.3.0` | 33975323568 | 2026-09-05 |

    `git show v0.3.0:.github/workflows/release.yml` shows what executed:
    `anchore/sbom-action/download-syft`, `sigstore/cosign-installer`,
    `id-token: write`, a buildx build, and `cosign sign` against an
    `ghcr.io/grace/switchboard` digest. Signing by digest requires the image to
    have been pushed first, so **signed images with SBOMs shipped, three times,
    not by hand.** Whether those ghcr packages still exist is unconfirmed: the
    available token lacks `read:packages`, so the runs are evidence they were
    published and not evidence they are still there.

    **The current file is a rewrite and none of it has executed.**
    `release.yml` was added on 2026-09-04 (`eadf81b`), removed, and re-added on
    2026-09-08 (`05edca5`); it differs from the v0.3.0 version by 71 insertions
    and 47 deletions. The differences are the substance: buildx's own
    `--sbom=true --provenance=mode=max` replaces the third-party syft action,
    every action is pinned by full commit SHA where before they floated on `@v0`
    and `@v3`, the target moved from ghcr to ECR with
    `aws-actions/configure-aws-credentials`, and a step verifies the signature it
    just made.

    That rewrite needs the OIDC role from
    `deploy/cloudformation/github-oidc.yaml`, which is written but not deployed,
    and it could not be rehearsed locally: the attestation flags require buildx
    0.10 or later on the `docker-container` driver, and the machine it was
    written on has buildx 0.8.2 with only `docker`-driver builders, on Docker
    Engine 20.10.17. What is verified is that the workflow parses, that every
    action is pinned to a SHA confirmed to be a real commit, and that the trigger
    admits tags only.
16. **Infrastructure assumptions.** `controlplane.yaml` requires an existing VPC,
    subnets, IAM roles, KMS keys, ECS cluster and load balancer target group.
    `quickstart.yaml` removes all of those except the certificate.

17. **Onboarding, largely closed.** `docs/LOCAL.md` now documents a file-only
    path to a first real provider request that needs no Postgres, no migrations,
    no database roles, no tenant, no principals and no control plane. It was
    verified by following it from a clean directory, using nothing that is not on
    the page, and it produced a real completion from OpenAI with
    `X-Switchboard-Provider: openai`.

    Four defects behind it are fixed. `config.example.json` could not start,
    because its trust key was not valid base64 and it declared four providers of
    which three demand a key variable; it is now a working file-only example.
    Nothing derived a public key from a signing seed, while `quickstart.yaml` and
    `docs/SECURITY.md` both instructed the reader to obtain one, so
    `gateway -public-key` now does it and both documents are corrected. The
    single `invalid configuration limits` covering thirteen conditions now names
    the field and its range. `controlplane.policytool` signs a policy straight to
    `data_dir/policy.json` and computes the timestamps, reaching the same
    `policy.sign` the control plane uses; a fixture test holds the Python signer
    and the Go verifier to one canonical encoding.

    **A worse defect surfaced while testing that.** `slog.Error` followed by
    `os.Exit` raced the async log writer, and a misconfigured gateway exited 1
    printing nothing at all roughly a third of the time, measured at 6 and 8
    successes in 10 runs. Every fatal path now flushes first, verified at 20 out
    of 20. A configuration error that prints nothing is the worst possible
    failure for the exact person this work is for, and the hazard was already
    known: `main.go` carried a comment about it on one path and not the other
    thirteen.

    Still open, recorded so it is not rediscovered: four correlated secrets
    related only by `.dev/env`.

    Two items that were listed here are since resolved. The 15 second policy
    poll was never the reason a first control-plane attempt looked like a hang —
    `Sync` calls `syncOnce` before waiting on the ticker, so the first poll is
    immediate; the reason was that `syncOnce` discarded eight distinct errors in
    silence, which item 20 covers. And `asm-exec` is gone from the product
    surface: it was an agent-side wrapper for resolving
    `{{resolve:secretsmanager:...}}` references, not a tool any reader has, and
    it had leaked into the customer runbook where `docs/DEPLOYMENT.md` steps 2
    and 4 told a subscriber to run a command that could not exist on their
    machine. Nothing replaces it — every value it was meant to resolve already
    arrives by ECS `secrets:` injection, or is composed in Python by
    `_migration_dsn()` so no shell interpolates a password into a URL.

    Not a defect and deliberately unchanged: `adapter.go` rejects any model but
    `preferred`, so the first thing an OpenAI SDK user types fails. That is the
    signed policy doing its job, and `LOCAL.md` now says so at the point the
    reader meets it.

18. **The request surface excludes every agentic workload, and this file did not
    say so.** `Chat` (`adapter.go:21-27`) models five fields and `strictJSON`
    (`policy.go:44-54`) sets `DisallowUnknownFields`, so `tools`, `tool_choice`,
    `functions`, `response_format`, `top_p`, `n`, `stop`, `seed` and
    `stream_options` are all a hard 400. `role: "tool"` and assistant
    `tool_calls` are rejected too (`adapter.go:44`), so a tool loop cannot be
    carried even if the tools were declared elsewhere. No MCP host, no coding
    agent, no RAG-with-tools and no extraction pipeline can sit behind this
    gateway.

    The response side is fail-closed to match, in five places
    (`adapter.go:311`, `:361`, `:335-345`, `:400-403`, `bedrock.go:331`,
    `:254`), plus `finish()` (`adapter.go:173`) which whitelists finish reasons
    so `tool_calls` fails independently of the guards. Item 9's note that
    Bedrock "tool and reasoning blocks are refused rather than flattened" is the
    same gap seen from the provider side.

    The rejection itself is correct and deliberate. What was wrong is that this
    file was silent about it while `README.md:15` sends deployment readers here.
    The disclosure existed only at `README.md:40` and at the end of an
    idempotency paragraph in `docs/API.md`. `docs/API.md` now carries an
    "Unsupported request surface" section above the endpoint table and
    `README.md:15` points at it.

    A second, smaller honesty problem in the same area: the control-plane RBAC
    role named `agent` (`docs/API.md`, `app.py:39`) is a principal that reads
    policy and ingests telemetry. It has nothing to do with AI agents, and a
    reader skimming the role table could reasonably conclude the opposite. Now
    stated inline.

19. **Policy schema-version negotiation, fixed; one precondition remains.** The
    verifier tested `p.Schema != policySchema` -- exact equality -- while the
    gateway advertises `max_schema=policySchema` and the control plane honours
    that as a ceiling (`schema<=%s`). A build with `policySchema = 2` would have
    asked for "2 or lower", been correctly served a tenant's newest schema-1
    policy, and rejected it; every gateway would then have served its cached copy
    and gone 503 when that expired, up to seven days later, fleet-wide -- the
    exact outcome the `max_schema` mechanism exists to prevent. Never reachable,
    because `controlplane/policy.py` refuses to sign anything but schema 1.

    Now `schemaSupported(n, ceiling)`, a range. The ceiling is a parameter rather
    than a direct read of the constant so the behaviour is testable today: at
    `policySchema = 1` a range and an equality are indistinguishable, so a test
    pinned to today's value could not tell the fix from the bug. Strict on write
    and permissive downward on read is deliberate -- signing an unknown schema is
    a mistake, verifying an older one is the job.

    **The precondition, for whoever ships a schema 2.** `Canonical()`
    hand-enumerates one field set and `verify()` requires
    `bytes.Equal(b, Canonical(p))`, so it must dispatch on `p.Schema` before two
    schemas can both verify. Python's `canonical()` is generic and needs no
    change. Everything else hardcoding the version or the field set:
    `controlplane/policy.py`, `controlplane/policytool.py`,
    `scripts/devstack.py`, `docs/DEPLOYMENT.md`, `003_policy_schema.sql`, the
    signed `testdata/` fixtures (regenerated by hand, see the note in
    `policy_test.go`), and `controlplane/tests/test_policy.py`, which asserts
    that schema 2 raises. Generate v2 fixtures **empty-list first, one-entry
    second**: a nil Go slice marshals to `null` where Python's `json.dumps([])`
    emits `[]`, and the checked-in fixtures would not catch it.

20. **Three defects recorded rather than fixed, with the reasoning.**

    **`idemStore.Sweep` holds the store mutex across a full `os.ReadDir` and an
    unbounded delete loop**, and runs every minute while `begin`/`finish`/`write`
    contend on the same mutex on the request path. `capture.go` documents this
    exact pattern as one it deliberately avoided -- "inherited from
    idempotency.go" -- and added both an unlocked scan and a `sweepPerPass`
    bound; the idempotency store got neither. Left because it is reachable only
    with `idempotency_ttl_seconds` set, which is off by default. The fix is to
    copy what `capture.go` already does.

    **Bedrock `usage` aliases the decoder's reused scratch buffer.**
    `translateBedrockStream` retains `msg.Payload` from the `metadata` event, and
    the vendored eventstream decoder builds payloads over the caller's buffer, so
    the next `Decode` overwrites it in place. Benign only because Bedrock sends
    `metadata` last and no further decode succeeds. It becomes silent billing
    corruption -- wrong token counts returned to the caller, no error -- the day
    AWS emits anything after `metadata` or reorders it before `messageStop`.

    **A stream that fails before its first byte reports 200 to the client and 502
    to telemetry.** `stream()` writes an SSE error frame, implicitly committing
    HTTP 200, while `chat()` sets `event.Status = 502`, so the span carries
    `error.type` and a 502 status code for a request the client observed as a
    200. Defensible as an SSE design -- the status is already sent and cannot be
    withdrawn -- but the wire and the telemetry disagree, and anyone reading an
    error rate is misled. Changing it would touch the streaming contract in
    `docs/API.md`, so it is written down instead.

21. **The enterprise carve-out is deferred, not dropped.** The project is
    Apache 2.0 as of 2026-09-09, having gone MIT → Elastic License 2.0
    (2026-09-07) → Apache. No `enterprise/` directory exists, deliberately: an
    empty carve-out establishes nothing, and one added later costs nothing,
    because it lives in `NOTICE` and `enterprise/LICENSE` and touches no
    existing path.

    The only current occupant would be `internal/gateway/marketplace.go` — 112
    lines, three touchpoints in `config.go` and three in `cmd/gateway/main.go`.
    Moving it needs a `//go:build enterprise` tag and a no-op stub so the
    default build genuinely excludes it, which is a code change and not a
    licence change. Everything else that might qualify is unbuilt.

    The principle, when it arrives: **gate operational scale and billing, never
    the security primitives.** The CEL layer, per-caller policy, and policy
    signing and verification stay in the open tree regardless — a free version
    that cannot enforce does not do what this project claims. Elastic made its
    security features free in 2019 after sustained criticism for exactly this;
    that is the precedent, not a hypothetical.

    If the carve-out is adopted, keep Apache verbatim in `LICENSE` and put the
    carve-out in `NOTICE`. A composite root `LICENSE` is why GitHub serves
    highlight.io as `License-1` instead of an `Apache-2.0` badge. The ELv2 text
    is recoverable at `git show f3d7236:LICENSE`.

22. **The relicense commit credits the DCO with something the DCO does not do.**
    `3b3fc4b` says "The DCO's inbound grant is what made this clean, as it did
    the previous one." The DCO has no inbound grant. It is a certification:
    clauses (a) to (d) attest that the contributor wrote the work and had the
    right to submit it under *the open source license*, and nothing in it
    licenses anything to anyone.

    What supplies the inbound license is Apache 2.0 section 5. What permits
    relicensing a contribution under different or proprietary terms -- the thing
    a future `enterprise/` carve-out and Switchboard Recordkeeper actually depend
    on -- is the separate **Grant** section of `CONTRIBUTING.md`, which is
    sublicensable and transferable and says so plainly. That architecture is
    correct; only the commit message's account of it is not.

    What in fact made both relicenses clean is simpler: contributions are not
    accepted, so one person holds the entire copyright. The DCO and the Grant
    matter from the first external contribution onward, not before it.

    Recorded rather than corrected, because the commit is already on three
    remotes and rewriting published history to fix a sentence is a worse trade
    than a note. `CONTRIBUTING.md` itself is accurate and needs no change.

## Blocked externally

These need an action from the owner or from AWS. **They are not implementation
defects, and nothing in the code is waiting on a fix.**

1. **AWS Marketplace seller registration**, including tax and banking details,
   and a defined customer support process — required for any listing, including
   a free one.
2. **A real `ProductCode`.** `RegisterUsage` is implemented and unit-tested
   across entitled, unentitled, unconfigured and client-failure paths, but it
   cannot be exercised against AWS until a listing exists. Registration against
   the live Marketplace metering service is therefore **untested**.
3. **A TLS certificate and DNS record.** The quickstart template cannot be
   deployed without an ACM certificate for a domain the deployer controls.
4. **Marketplace-managed ECR.** A listing requires every image a subscriber needs
   to be published there. The current images are in a development registry.
5. **A full deployment.** Standing up the quickstart stack costs roughly
   $180-200 per month while it runs, and is a spending decision rather than an
   engineering one.
