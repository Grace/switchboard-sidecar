# Deployment

## Preparation

The modules create billable resources when you run Terraform. Nothing was provisioned while producing this repository. Use a separate reviewed root configuration with a remote encrypted state backend, locking, explicit AWS region and standard tags. Run `terraform init`, `terraform validate`, then review a saved plan before apply.

Use private subnets across at least two Availability Zones. Customer tasks need controlled outbound HTTPS to provider APIs and the control plane. Control-plane tasks need Postgres access and outbound access as required by your deployment. ECR/Logs/Secrets endpoints or NAT are needed for task startup. Do not expose the gateway's port 8080 outside the task.

## Postgres and control plane

1. `deploy/terraform/postgres` creates an encrypted Multi-AZ RDS PostgreSQL instance, private subnet group, client-SG-only port 5432 access, forced TLS, 14-day backups and deletion protection. Choose an engine version available in your region. Pass existing KMS key and subnet/security-group IDs. RDS manages the master password; only its secret ARN is output. The final snapshot name must be unique if you later replace a previously deleted deployment.
2. From a trusted migration runner, apply `python -m controlplane.migrate` with `MIGRATION_DATABASE_URL` set from your secret store. Use a dedicated database/cluster. Migrations create the `switchboard_app` role, schemas, RLS and security-definer helpers. They run under a transaction/advisory lock and reject changed migration checksums.

   On ECS this is a task using the control-plane image with a `secrets:` block, which is what `deploy/cloudformation/quickstart.yaml` runs. Do not assemble the DSN on a command line: `scripts/devstack.py dbinit` composes it in Python from `DBHOST` and a `PGPASSWORD` injected by ARN, so the password never becomes part of a URL in a shell, a process table, or a CloudTrail `RunTask` event. Anywhere else, inject the variable by whatever means your platform provides and keep the value off the command line.
3. Create a separate runtime login in your approved database credential workflow, with no superuser, role-creation, database-creation or RLS bypass privileges. Grant it `switchboard_app`. Store its full connection string with `sslmode=verify-full` and a CA path in the control-plane secret. Migration credentials must not be installed in the long-running service.
4. Provision the first tenant with `python -m scripts.bootstrap acme-prod`, with `MIGRATION_DATABASE_URL` and `BOOTSTRAP_ADMIN_TOKEN` injected the same way as step 2 — a task with a `secrets:` block on ECS, your own injection mechanism elsewhere. Create the bootstrap token yourself, at least 32 bytes of entropy, and store it in your secret manager before running this. The script inserts only its SHA-256 digest and never prints the token. The bootstrap admin expires after **seven days**; use it to register normal-role credentials, then revoke it.
5. Build `Dockerfile.controlplane`; create a derivative image containing the public RDS CA bundle at the path in the connection string. Scan and pin by digest. Do not embed a connection string or signing seed in the image.
6. `deploy/terraform/controlplane` registers the task and runs two Fargate replicas against an **existing** target group, cluster, execution/task roles, private subnets and task security groups. Pass `DATABASE_URL` and `POLICY_SIGNING_SEED` in `secret_arns`, plus `policy_key_id`. It does not run migrations at every application startup.
7. Attach the target group to an existing ALB HTTPS listener with an ACM certificate. Target type must be `ip`, port 8000, health path `/readyz`; only the ALB SG may reach the control tasks. Configure edge rate limiting/WAF and an appropriate ALB idle timeout. The application itself has concurrency/body/pool bounds, but does not implement a distributed control-plane rate limiter.
8. Publish a tenant policy using `PUT /v1/policy` as an admin/publisher. See the body format below. Register an `agent` principal for the sidecar, independently of the local application credential.

```json
{
  "schema": 1,
  "tenant": "acme-prod",
  "version": 1,
  "issued_at": 1800000000,
  "expires_at": 1800003600,
  "routes": [{"provider":"openai","model":"YOUR_APPROVED_MODEL"}]
}
```

Replace timestamps with current Unix seconds and approved model identifiers. These illustrative values are not a bootstrap policy. Publish and verify before starting dependent applications, or let the gateway sync at startup. An initial control-plane outage with no cached policy fails readiness; existing valid caches remain usable.

## Customer sidecar

1. Copy `config.example.json` to an ignored `config.json`. Set tenant, control URL, public verification keys and the provider subset you actually configure. Remove unused providers so startup does not require their credentials. Local and control tokens are distinct, high-entropy secrets.
2. Build `Dockerfile.gateway`, then a tenant-specific derivative image:

```dockerfile
FROM your-registry/switchboard-gateway@sha256:REVIEWED_DIGEST
COPY config.json /etc/switchboard/config.json
```

3. Pin that image digest in `deploy/task-definition.json` or consume `deploy/terraform`'s `container_definition` output in your existing task. The task example includes a small reviewed init container to set the writable volume's ownership. Replace its image placeholder with a digest-pinned image. The init runs only `chown`/`chmod`; the gateway itself runs as UID/GID 65532 with all Linux capabilities dropped and a read-only root filesystem.
4. Provide execution-role access to exactly the secret ARNs and relevant KMS keys, ECR pull and log writing. No runtime task-role AWS permissions are required for the default gateway. The app receives the local token as its OpenAI SDK credential, never provider credentials. Add a `HEALTHY` dependency on `switchboard`.
5. Use `awsvpc`, Fargate Linux platform 1.4.0 or a supported later platform, and allocate task CPU/memory for all containers. App requests go to `http://127.0.0.1:8080/v1`; configure SDK retries to zero. The sidecar uses `/gateway --healthcheck`, which needs no shell, curl or wget.
6. Add the Collector as a same-task container if wanted. Set `otlp_url` to `http://127.0.0.1:4318/v1/traces` and `allow_local_http` to true. `deploy/collector.yaml` scrapes the loopback Prometheus endpoint and exports traces/metrics to your configured backend. Review resource limits and exporter authentication independently.

## Durability choice

The task example uses ephemeral storage. Its policy/spool survive a container restart within the task but disappear on task replacement. This is **not lossless telemetry across task termination**. The bounded memory queue may also lose admitted events on abrupt process death. Metrics count observed queue/disk drops; a killed process cannot count events it never persisted.

For persistent cache/spool across task replacement, mount an encrypted EFS access point at `/data`, enforce UID/GID 65532, enable transit encryption and IAM authorization, and restrict mount-target TCP 2049 to the task SG. Grant only `ClientMount`/`ClientWrite` for that access point. Omit the chown init when EFS access-point ownership supplies permissions.

**One data directory must have exactly one running gateway.** A process file lock enforces that boundary. Use a stable independently managed task slot/access point per sidecar and stop the old owner before starting its replacement. Do not point a rolling/scaled ECS service's replicas at the same directory. EFS orchestration/scavenging for arbitrarily scaled replicas is not included; retain the default ephemeral deployment only if its telemetry-loss semantics are acceptable.

## Monitoring

**By default nothing collects the gateway's metrics.** `/metrics` is bound to loopback with the rest
of the gateway, and `deploy/collector.yaml`, which scrapes it, is an opt-in container present in
neither CloudFormation template nor the sample task definition. Until one of these is done, every
counter below is unreadable:

- Set `otlp_metrics_url` to your OTLP/HTTP metrics endpoint. The gateway pushes every counter and
  gauge every 30 seconds, needing no scraper and no extra container, including the request-duration
  histogram. Counters and the histogram are cumulative.
- Or add the Collector as a same-task container, per step 6 above.

### Sending metrics to Honeycomb

Honeycomb is the default path: the gateway already speaks OTLP, and Honeycomb ingests it directly,
so alerting is Triggers on the counters rather than infrastructure this project has to provision.

```jsonc
"otlp_metrics_url": "https://api.honeycomb.io/v1/metrics",
"otlp_url":         "https://api.honeycomb.io/v1/traces",
"otlp_headers":     { "x-honeycomb-team": "HONEYCOMB_API_KEY" }
```

**The header value is the name of an environment variable, not the key.** `config.json` is mounted
from a volume and readable by anything in the task, and this is how `key_env`, `control_token_env`
and `local_token_env` already work. Supply `HONEYCOMB_API_KEY` the same way you supply provider keys.

The gateway refuses at startup if a named variable is empty, if a header name is not a valid HTTP
token, or if the map tries to override `Content-Type` or `Authorization`.

The same mechanism reaches anything else that speaks OTLP over HTTP. Grafana Cloud takes Basic auth,
Datadog takes `dd-api-key`, and a local collector takes no header at all.

### Triggers worth creating

These four map to something a person should do. **The Honeycomb free plan allows two triggers per
team**, and a third create returns `exceeded maximum 2 triggers for this team's plan`. All four
conditions still fit; see "Four conditions in two triggers" below.

**The column names differ by path, and this is the detail that breaks triggers.** The Prometheus
exposition on `/metrics` separates words with underscores — `switchboard_empty_completion_failed_total`.
The OTLP exporter emits `switchboard.` plus the name, so what actually arrives in Honeycomb is
dotted. Build a trigger from the `/metrics` spelling and it references a column that is not there.
The table below uses the Honeycomb spelling.

| Column (Honeycomb) | Means | Urgency |
|---|---|---|
| `switchboard.empty_completion_failed_total` | Every route produced no output; the caller got a 503 | **Page.** This is lost service. |
| `switchboard.account_failover_total` | A provider account cannot serve: no credits, balance too low, quota gone | Notify. Usually a billing action, not an incident. |
| `switchboard.usage_mismatch_total` | A provider's token totals did not add up, so billing figures may be wrong | Notify. Any non-zero value deserves a look. |
| `switchboard.idempotent_unknown_total` | A retry was refused because the original outcome was ambiguous | Notify. Rising means real ambiguity is being caught rather than paid for twice. |

These are cumulative counters. Aggregate with **`SUM`** and fire above zero: on a Metrics dataset
Honeycomb applies the counter's own temporal aggregation before yours, so `SUM` over the trigger
window is the increase during that window rather than the running total.

**Do not use `RATE_SUM` here**, whatever a rate-shaped counter suggests. Honeycomb refuses it on a
Metrics dataset outright — `aggregate operation not allowed in Metrics dataset: RATE_SUM` — and the
refusal is easy to miss, because tooling sitting in front of the API may surface nothing more than
`Failed to save trigger`. A trigger already holding a `RATE_SUM` query is worse still: it displays
as healthy and never evaluates, because the query it runs is one the engine will not accept. Check
an existing trigger by running its query by hand; if the query errors, the trigger is dead.

`COUNT` measures how often the exporter reported rather than what it reported, and `MAX` on a
counter that only ever climbs stays above any threshold once crossed.

`switchboard.empty_completion_recovered_total` is worth a graph rather than a trigger: it is spend
and latency, not an outage, and budget-aware routing should drive it toward zero on its own.

### Where the alert conditions live

`controlplane/alerting.py` holds the four conditions, what each one means, and which of them is an
outage. It contains no vendor: no API calls, no query syntax, and **no metric prefix**. Two emitters
read it.

| Emitter | Backend | Spelling it applies | Alerts produced |
|---|---|---|---|
| `controlplane/honeycomb.py` | Honeycomb, over OTLP | `switchboard.<name>` | 2 triggers |
| `controlplane/prometheus.py` | Anything scraping `/metrics` | `switchboard_<name>` | 4 rules |

The two counts are not a discrepancy. There are four conditions; Honeycomb's free plan allows two
triggers per team, so that emitter folds the three notify conditions into one formula. Prometheus
has no such cap and gets one rule each. **The folding is a compromise with a vendor's pricing, not
something Switchboard believes about its own counters**, which is why it lives in the emitter.

The prefixes are the other reason for the split. The gateway publishes every counter twice, and the
two paths spell it differently: `/metrics` writes `switchboard_empty_completion_failed_total`, OTLP
writes `switchboard.empty_completion_failed_total`. Building an alert against the wrong one produces
a rule referencing something that does not exist, which Honeycomb accepts and never fires. Holding
the bare name in one place and letting each emitter apply its own spelling makes that unwritable
rather than merely documented.

A third backend is a third emitter reading the same conditions, not an edit to any of this.

### Prometheus

```sh
python -m controlplane.prometheus --out switchboard.rules.yml
```

Writes standard alerting rules, using `increase(...[window]) > 0` rather than a rate: these are rare
events, and a rate turns "one account failed over" into a small fraction with a threshold nobody can
reason about. The generated file also lists, as comments, the counters that are deliberately *not*
alerted and why — `empty_completion_recovered_total` above all, since the caller was served.

### Honeycomb

`controlplane/honeycomb.py` creates the recipient, both triggers and the board in any
Honeycomb environment, and is safe to re-run: everything is matched by name and updated in place.

```sh
export HONEYCOMB_CONFIG_KEY=...     # a Configuration key
python -m controlplane.honeycomb --dataset Metrics --recipient ops@example.com
python -m controlplane.honeycomb --dataset Metrics --recipient ops@example.com --dry-run
```

**Not `HONEYCOMB_API_KEY`.** That variable holds the *ingest* key the gateway itself reads to
authenticate its OTLP export, and wherever the gateway can read it, a configuration key would let
the data plane rewrite or delete the alerting that watches it. The tool reads its own variable and
does not fall back.

The key needs **Manage Triggers, Manage Boards, Manage Recipients and Run Queries**; a new
Configuration key does not have all four by default. The tool calls `/1/auth` before writing
anything and names any that are missing, along with the team and environment the key actually points
at — a key from the wrong environment otherwise provisions a perfectly correct set of triggers
somewhere nobody is looking.

`--recipient` is required unless you pass `--no-recipient`. **A trigger with no recipient notifies
nobody** while still appearing healthy in the trigger list, which is worse than having no trigger at
all because it reads as coverage on a dashboard. Silence should be something you typed.

The tool executes each trigger's query before exiting, and fails if one does not run. That is not
belt-and-braces: Honeycomb accepts a trigger whose query the engine refuses, lists it as healthy,
and never fires it. Creating a trigger is not evidence that the trigger works.

### Four conditions in two triggers

A trigger query may hold only one aggregate — a second calculation is refused with `query: only one
non-having aggregate is allowed` — but a **formula** collapses several into one value, and formulas
do work on a Metrics dataset. So one trigger can watch several counters:

| Slot | Query | Urgency |
|---|---|---|
| 1 | `SUM(switchboard.empty_completion_failed_total)` > 0 | **Page.** |
| 2 | `SUM(account_failover) + SUM(usage_mismatch) + SUM(idempotent_unknown)` > 0 | Notify. |

Leave the page alone in slot 1. It is the only one of the four that is an outage, and merging
anything into it blunts the only alert that should wake someone.

The cost is that slot 2 says *something moved* rather than *this moved*. Pay for that with a board
rather than a plan: boards are not capped, and a panel graphing the three counters separately turns
the notification into a ten-second lookup. Put the counter names in the trigger description too,
since that text is what arrives in the notification.

```json
{
  "calculations": [
    {"column": "switchboard.account_failover_total",   "op": "SUM", "name": "failover"},
    {"column": "switchboard.usage_mismatch_total",     "op": "SUM", "name": "mismatch"},
    {"column": "switchboard.idempotent_unknown_total", "op": "SUM", "name": "unknown"}
  ],
  "formulas": [{"name": "needs_a_human", "expression": "$failover + $mismatch + $unknown"}],
  "time_range": 900
}
```

### Anomaly detection will say your service is ineligible, and that is expected

Honeycomb can watch a service's error rate without a threshold, and it will refuse to watch a new
deployment. **It requires roughly 85% data coverage** and re-checks eligibility **weekly**, so
expect `ineligible` for at least the first week, and indefinitely for a service whose traffic is
intermittent. The banner it shows is written by the last weekly check, so it can describe a state
that has already been fixed; read the coverage percentage on the Presence tab rather than the
sentence on the Error Rate tab.

Two consequences worth knowing before you read that banner as a fault in the gateway:

- Coverage counts spans, and spans are emitted per request. A gateway that is running but idle
  produces none, so uptime alone does not accumulate coverage. Real traffic does.
- Its error rate is computed from `error`, `error.message`, `error.type`, `exception.message` and
  `exception.type` — **not** from the span status. The gateway emits `error.type` on every 4xx and
  5xx and Honeycomb derives `error` from it, so nothing needs configuring; but a service that only
  sets a span status will be told it has no recognised error attributes, which is a confusing way
  to be told a true thing.

The triggers above carry none of this. They evaluate on their own schedule with no coverage
requirement, which is the reason to set them up rather than wait for detection to turn itself on.
It will, once coverage recovers, and nothing needs re-configuring when it does.

**The plan tier is not what stands in the way.** Coverage measures whether data is *present* across
evaluation windows, not how much of it there is, and Honeycomb's free plan allows 20M events and
100M metric datapoints a month. The traffic needed to satisfy coverage costs almost none of that:

| Traffic | Monthly | Share of the free allowance |
|---|---|---|
| One request a minute | ~43,000 spans | 0.2% of 20M |
| One request a second | ~2.6M spans | 13% of 20M |
| Metrics alone, running continuously at the 30s export interval | ~2.4M datapoints | 2.4% of 100M |

What is actually required is a service that runs and receives requests more or less continuously.
Metrics do not help: coverage for the traces dataset counts spans, and spans are emitted per
request, so an idle gateway accumulates none however long it stays up.

A synthetic request every minute would satisfy coverage cheaply. It would also dilute the error rate
that detection is baselining, so whether that is worth doing depends on how much real traffic it
would be diluting — which is a decision to take with traffic in hand, not in advance.

### What to alarm on

| Metric | Meaning | Threshold |
|---|---|---|
| `empty_completion_failed_total` | Every route produced no output and the caller got a 503. **Lost service.** | Low. Any sustained rate is user-visible failure. |
| `empty_completion_recovered_total` | A later route answered after one produced nothing. The caller was served, but paid two providers and waited for both. **Wasted spend and latency.** | Higher, and as a rate against `requests_total`. |
| `account_failover_total` | A provider account could not serve: no credits, balance too low, quota exhausted. | Low. This is usually a billing action, not an incident. |
| `provider_probe_failed_total` | The startup check rejected a provider. | Any non-zero value warrants a look. |
| `usage_mismatch_total` | A provider's own token totals did not add up, so billing figures may be wrong. | Any non-zero value. |

`empty_completion_total` is counted **per route**, so one request can advance it more than once. Use
the recovered and failed counters for alarms; that one cannot tell the two apart.

On what counts as "regularly": reasoning models spend their token budget on hidden reasoning before
writing an answer and reserve nothing for it. Measured against `gpt-5-nano` at the 1024 default,
**four of seven ordinary prompts returned no text at all**. A policy routing to a reasoning model
should expect a non-trivial rate here, and the fix is usually a higher `max_tokens` from the caller
rather than anything in the deployment.

## Operations and rollback

Alert on policy refresh errors/approaching expiry, inference errors, rejections, dropped telemetry, export errors and spool growth. Poll authenticated `/runtime` for policy expiry. A control-plane outage is nonfatal only until the cached policy expires. Replace secret-injected tasks after rotation. Test draining with a long stream and prove app-stop ordering before release.

Keep Postgres point-in-time recovery and restore drills. Retain telemetry with a scheduled trusted maintenance job; tables are not automatically partitioned. Delete old telemetry in small batches after your agreed retention period, and retain audit/policy records according to customer requirements. Runtime role cannot delete audit data.

Roll back an image by redeploying the previous digest only if schema compatibility has been checked. Roll back routing by publishing a **higher** policy version. Do not edit old migration files or restore old policy-cache snapshots over newer versions. Use forward migrations; automated destructive down migrations are intentionally absent.

## Reference images

Built `linux/amd64` and pushed 2026-09-07. Both CloudFormation templates pin by
digest, so these are the values to supply as `ControlPlaneImage` and to reference
from the sidecar task definition. ARM64 is not built: it matters for EKS add-on
delivery, which is not the chosen distribution path.

| Image | Digest |
|---|---|
| `switchboard/gateway:0.1.0` | `sha256:52069ca601b4fad0704f158c29ea4f5e8a90d20b2bfd522e2e5aa64d943cb94b` |
| `switchboard/controlplane:0.2.0` | `sha256:b8a6153a0e5a89875cf84ebd41c5d41eb7c1041d75326c15d040aa6aae599eb3` |

These live in a development registry. A Marketplace listing requires every image
a subscriber needs to be pushed to AWS Marketplace managed ECR instead.

**Image scanning.** Both images report **no findings**. The control plane was
rebased onto distroless in `0.2.0`, which removed the 19 findings its
Debian-based `0.1.0` carried — all of them in operating-system packages with no
upstream fix available, 13 of them in perl and util-linux, neither of which the
application used.

Note what this does and does not mean: ECR scans operating-system packages, so
the OpenSSL vendored inside the `psycopg` and `cryptography` wheels is outside
its scope. A clean scan is a much smaller attack surface, not a guarantee.

## CloudFormation templates

| Template | Required parameters | Capabilities |
|---|---|---|
| `quickstart.yaml` | `CertificateArn`, `ControlPlaneHostname`, `ControlPlaneImage` | `CAPABILITY_IAM` |
| `controlplane.yaml` | 14, all pre-existing infrastructure | `CAPABILITY_IAM` |
| `github-oidc.yaml` | `OidcProviderArn` | **`CAPABILITY_NAMED_IAM`** |

### Deploying the quickstart

Use `scripts/deploy.sh`. It is the only path carrying the properties a hand-run
`aws cloudformation deploy` does not:

```sh
./scripts/deploy.sh \
  --certificate arn:aws:acm:us-east-1:123456789012:certificate/12345678-1234-1234-1234-123456789012 \
  --hostname switchboard.example.com \
  --image <account>.dkr.ecr.<region>.amazonaws.com/switchboard/controlplane@sha256:<digest>
```

It refuses to deploy into a stack that is already wedged, runs
`scripts/preflight.sh`, and creates with `--disable-rollback`. That flag is the
one that matters, for the reason below. Updates to an existing stack go through
a normal change set with rollback left on, because by then the database exists,
is `available`, and can be snapshotted.

### Recovering a failed or wedged stack

A quickstart create that fails in its first ten minutes does not clean up after
itself. The reason is worth knowing before you meet it.

The database carries `DeletionPolicy: Snapshot`, so CloudFormation snapshots it
on the way out. It cannot snapshot an instance that is still `creating` -- and a
rollback of a failed create always arrives while the instance is still creating.
So the delete fails, and the rollback fails with it:

```
Database   DELETE_FAILED    Cannot create a snapshot because the database
                            instance <stack>-postgres is not currently in the
                            available state.
<stack>    ROLLBACK_FAILED  The following resource(s) failed to delete: [Database].
```

A stack in `ROLLBACK_FAILED` cannot be updated. There is no fixing it in place;
it has to be deleted and recreated. This is what `--disable-rollback` avoids: the
stack stops at `CREATE_FAILED` with everything intact, and the delete you issue
afterwards finds the database settled and snapshots it cleanly.

| Status | What it means | Way out |
|---|---|---|
| `CREATE_FAILED` | Created with `--disable-rollback`, so nothing was torn down | Read the events and logs, then `delete-stack` |
| `ROLLBACK_FAILED` | Rollback could not delete the database | `delete-stack`, which succeeds once the instance is `available` |
| `DELETE_FAILED` | Deletion protection, or a snapshot that cannot be taken | Clear protection, then `delete-stack`; failing that `--retain-resources Database`, then `--deletion-mode FORCE_DELETE_STACK` |

`./scripts/teardown.sh <stack>` prints these commands with your stack name and
region filled in, and refuses to touch anything while the stack still exists.

Two things bite on the retry:

- **Deletion protection.** `Environment=production` turns it on, and
  `delete-stack` then fails with `Cannot delete protected DB Instance`. Clear it
  with `aws rds modify-db-instance --db-instance-identifier <stack>-postgres
  --no-deletion-protection --apply-immediately`. The parameter defaults to
  `evaluation`, which leaves it off; the snapshot preserves the data either way,
  so what deletion protection buys is a guard against an accidental delete, not
  durability.
- **Final snapshot names must be unique.** As noted under Preparation, replacing
  a previously deleted deployment fails if an old final snapshot still holds the
  name. `scripts/teardown.sh` lists and removes them.

And the failure that prompted this section: `CertificateArn` used to end in
`.+`, which accepted two ARNs joined by whitespace -- the shape
`aws acm list-certificates --output text` returns when more than one certificate
matches. CloudFormation took the pair and only the load balancer rejected it,
four minutes in, after the VPC, NAT gateway and database had started building.
The template pattern and `scripts/preflight.sh` both reject it now. Pass exactly
one ARN.

### Putting your other services in the same vocabulary

Switchboard's spans follow the OpenTelemetry GenAI semantic conventions directly,
so nothing has to translate them. Your other services may not: OpenLLMetry,
OpenInference, the Vercel AI SDK, LiteLLM and Braintrust each name the same token
count differently, and a dashboard cannot span all of them at once.

[`genai-interlingua`](https://github.com/Grace/genai-interlingua) normalizes those
dialects into the same `gen_ai.*` model Switchboard emits, as a Collector
processor or a CLI. It is not a dependency of this product and nothing here
requires it -- but if you run it in front of your collector, one query answers
"what did this cost" across every service regardless of what instrumented it.

If you would rather not run another binary, `interlingua -emit ottl` prints a
`transform` processor configuration you can paste into the Collector you already
have. The emitted config states in its own header which attributes that route
cannot carry, which is the honest part: a subset, and it says which subset.

Switchboard's own test suite points the same tool back at this gateway and
asserts it finds nothing to translate and nothing lossy. See
`internal/gateway/conformance_test.go`.

### Read the OIDC subject before deploying the publishing role

`github-oidc.yaml` writes a trust policy matching the subject claim GitHub puts
in its token. **Do not assume that claim is `repo:OWNER/REPO`.** GitHub now
issues ID-qualified subjects — `repo:owner@14958021/repo@1356690158` — whose
numeric suffixes survive a rename of either. Ask the repository what it sends:

```sh
gh api /repos/OWNER/REPO/actions/oidc/customization/sub --jq .sub_claim_prefix
```

Pass a non-classic result as `SubjectPrefix`. Getting this wrong fails as
`Not authorized to perform sts:AssumeRoleWithWebIdentity`, which reads as a
permissions problem and is not one: the trust policy, the audience, the provider
and the workflow's `id-token: write` are all correct and the condition is simply
false. The action retries about a dozen times before giving up, so it also looks
like propagation. It is not.

The stack's `TrustedSubject` output prints what the role actually enforces.
Compare it against the command above before pushing a tag.

All three create IAM resources, so a deploying buyer must acknowledge
`CAPABILITY_IAM` — except `github-oidc.yaml`, which needs `CAPABILITY_NAMED_IAM`
because it sets `RoleName` explicitly. A custom name is what escalates the
requirement: CloudFormation makes you acknowledge it because a named IAM
resource can collide with another stack's and cannot be replaced without
disruption. The other two let CloudFormation generate names, which is why
`CAPABILITY_IAM` is enough for them.

`controlplane.yaml` creates only the sidecar metering policy; `quickstart.yaml`
additionally creates the execution and task roles; `github-oidc.yaml` creates
only the role GitHub Actions assumes to publish images. Every quickstart
parameter except the certificate, hostname and image has a default.

The certificate cannot be created by either template: the sidecar refuses plain
HTTP to anything but loopback and trusts only the system roots, so reaching the
control plane requires a publicly trusted certificate, and issuing one needs a
domain the deployer controls.

**Marketplace metering.** `RegisterUsage` is called by the sidecar, not the
control plane, so the permission belongs on whichever task role runs the sidecar
— not on any role these templates own. `quickstart.yaml` therefore publishes it
as an attachable managed policy and exports the ARN as
`SidecarMeteringPolicyArn`. It is required only for a metered subscription; free
and BYOL listings do not call `RegisterUsage` at all.
