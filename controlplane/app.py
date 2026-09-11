import base64
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
import pathlib
import re
import time
import uuid
from typing import Any, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from psycopg.rows import dict_row
from psycopg.conninfo import conninfo_to_dict
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from controlplane.policy import sign, validate_policy, ID
from controlplane.replay import reconstruct

log = logging.getLogger("switchboard")
log.setLevel(logging.INFO)
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(handler)
    log.propagate = False

class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

class NewPrincipal(Strict):
    # Caller generates and stores a random 256-bit token; only its SHA-256 enters this API.
    token_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    role: Literal["admin", "publisher", "viewer", "agent"]
    expires_at: int

class Event(Strict):
    id: str = Field(pattern=r"^[0-9a-f]{32}$")
    request_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    trace_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    span_id: str = Field(pattern=r"^[0-9a-f]{16}$")
    parent_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{16}$")
    provider: Literal["", "openai", "anthropic", "gemini", "bedrock"]
    status: int = Field(ge=100, le=599)
    # Not le=3. The gateway's own max_attempts is validated 1..3 in config.go,
    # but that is a local operating choice and this is a wire contract: encoding
    # today's retry ceiling here means raising it later rejects every event from
    # a gateway that does. Bounded against garbage, not against a policy.
    attempts: int = Field(ge=0, le=64)
    start_ns: int = Field(ge=1)
    end_ns: int = Field(ge=1)
    # Optional, and that is load-bearing in both directions. This model forbids
    # extras, so a gateway sending a field an older control plane does not know
    # fails every event: new fields require the control plane to deploy first.
    # Making it optional handles the other ordering, where an older gateway sends
    # nothing, so a rollback of the gateway alone does not stop telemetry.
    #
    # It is the join key back to the policies table, which keeps every signed
    # envelope by (tenant, version) forever. With it a request becomes a provable
    # routing decision; without it the envelope is there and nothing points at
    # which one was live.
    policy_version: int | None = Field(default=None, ge=1)
    # Everything the core schema does not name, kept verbatim.
    #
    # extra="forbid" on the core is deliberate and stays: a typo in request_id
    # should be a rejected event, not a silently mis-stored one. But a strict
    # model across an upgrade boundary the vendor does not control is a
    # different thing, and it has already fired once -- adding policy_version
    # made every event from a newer gateway fail validation on an older control
    # plane, and the gateway's response is to drop and count, so the symptom is
    # silence rather than an error anyone sees.
    #
    # A named object rather than extra="allow" for the whole model, because that
    # would buy forward compatibility by giving up the typo protection on the
    # fields that matter. New optional fields go in here; the core grows only
    # when both halves can be released together.
    ext: dict[str, Any] = Field(default_factory=dict)

class HealthReport(Strict):
    """One gateway's current statement about one provider.

    Only the two fault classes that can be true for someone else. A refused
    fault is one deployment's wrong key and a terminal fault is one caller's bad
    request; neither says anything about the provider, and the Literal here is
    what stops either being reported at all.
    """
    provider: Literal["openai", "anthropic", "gemini", "bedrock"]
    fault: Literal["account", "degraded"]
    # How long the reporter thinks its own cooldown lasts. Bounded on read, so a
    # buggy or hostile gateway cannot park a provider indefinitely.
    ttl_seconds: int = Field(ge=1, le=300)


class HealthSync(Strict):
    # Random per process, not persisted. It exists only to count distinct
    # agreeing instances inside one window.
    instance_id: str = Field(pattern=r"^[0-9a-f]{16}$")
    # Empty is the normal case: a healthy gateway still polls, because polling
    # is how it hears about everyone else.
    reports: list[HealthReport] = Field(default_factory=list, max_length=8)


class EventBatch(Strict):
    # Bounded in the model rather than checked by hand: an unbounded array is a
    # denial-of-service vector against a route that inserts every element, and a
    # limit the framework enforces cannot be forgotten at a call site.
    #
    # Elements are dicts, not Events, deliberately. Typing them as Event would
    # make one schema-invalid element reject the entire request with 422, and the
    # sender retries whatever was not acknowledged, so a single bad event would be
    # resent every tick forever with every other event stuck behind it. Each
    # element is validated individually in the handler instead, where a failure
    # costs that event and nothing else.
    events: list[dict] = Field(min_length=1, max_length=200)

def require(principal: dict, *roles):
    if principal["role"] not in roles:
        raise HTTPException(403, "forbidden")

def create_app(pool=None, seed=None, key_id=None):
    @asynccontextmanager
    async def lifespan(app):
        owned = pool is None
        if owned:
            dsn = os.environ["DATABASE_URL"]
            if os.getenv("ALLOW_LOCAL_HTTP") != "true" and conninfo_to_dict(dsn).get("sslmode") != "verify-full":
                raise RuntimeError("DATABASE_URL must specify sslmode=verify-full")
            app.state.pool = ConnectionPool(dsn, min_size=1, max_size=10, timeout=3, max_waiting=32,
                                            kwargs={"row_factory": dict_row, "connect_timeout": 5}, open=False)
            app.state.pool.open(wait=True, timeout=10)
        else:
            app.state.pool = pool
        app.state.seed = seed if seed is not None else base64.b64decode(os.environ["POLICY_SIGNING_SEED"], validate=True)
        app.state.key_id = key_id or os.environ["POLICY_KEY_ID"]
        if len(app.state.seed) != 32 or not ID.fullmatch(app.state.key_id):
            raise RuntimeError("invalid signing configuration")
        app.state.ready = True
        yield
        app.state.ready = False
        if owned:
            app.state.pool.close()

    app = FastAPI(title="Switchboard control plane", version="1.0.0", lifespan=lifespan,
                  docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def guard(request: Request, call_next):
        rid = uuid.uuid4().hex
        started = time.monotonic()
        # Bound bodies even for chunked uploads; never log body or auth header.
        size = 0
        chunks = []
        try:
            async with asyncio.timeout(10):
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > 65536:
                        return JSONResponse({"detail": "body too large"}, status_code=413)
                    chunks.append(chunk)
        except TimeoutError:
            return JSONResponse({"detail": "request timeout"}, status_code=408)
        request._body = b"".join(chunks)
        try:
            response = await call_next(request)
        except Exception as exc:
            # The type and the traceback, not just a request id. Without them a
            # control plane whose database has drifted answers every gateway
            # identically and names only an id, while the gateway's own policy
            # sync counts an error -- between the two, nothing anywhere says what
            # happened. exc_info goes to the handler so the traceback survives;
            # the type and message go in the JSON so a log search can find them.
            #
            # The message is deliberately not returned to the caller: it can
            # quote a DSN or a query. It goes to the operator's log only.
            log.error(json.dumps({"event": "request_failed", "request_id": rid,
                                  "error_type": type(exc).__name__,
                                  "error": str(exc)[:300]}), exc_info=exc)
            # 500, not 503. This is an unhandled fault, and 503 told every caller
            # it was worth retrying -- which for a deterministic bug means the
            # same failure at the same rate forever, and hides it behind what
            # looks like transient unavailability.
            response = JSONResponse({"detail": "internal error"}, status_code=500)
        response.headers["X-Request-ID"] = rid
        log.info(json.dumps({"event": "request", "request_id": rid, "method": request.method,
                             "status": response.status_code, "duration_ms": int((time.monotonic()-started)*1000)}))
        return response

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        return JSONResponse({"detail": "invalid request schema"}, status_code=422)

    def session(request: Request, authorization: str = Header(default="")):
        if not authorization.startswith("Bearer ") or not 32 <= len(authorization[7:]) <= 512:
            raise HTTPException(401, "unauthorized")
        digest = hashlib.sha256(authorization[7:].encode()).hexdigest()
        with request.app.state.pool.connection() as db, db.transaction():
            db.execute("SET LOCAL statement_timeout='3000ms'")
            db.execute("SET LOCAL lock_timeout='1000ms'")
            p = db.execute("SELECT * FROM authenticate(%s)", (digest,)).fetchone()
            if p is None:
                raise HTTPException(401, "unauthorized")
            db.execute("SELECT set_config('app.tenant',%s,true),set_config('app.role',%s,true)", (p["tenant_id"], p["role"]))
            yield db, p

    def audit(db, p, action, detail):
        db.execute("INSERT INTO audit(tenant_id,principal_id,action,detail) VALUES(%s,%s,%s,%s)",
                   (p["tenant_id"], p["id"], action, Jsonb(detail)))

    @app.get("/healthz")
    def health():
        return {"ok": True}

    @app.get("/readyz")
    def ready(request: Request):
        with request.app.state.pool.connection() as db:
            db.execute("SELECT 1").fetchone()
        return {"ready": request.app.state.ready}

    @app.get("/v1/policy")
    def policy(max_schema: int | None = None, s=Depends(session, scope="function")):
        """The newest policy this caller can actually verify.

        A gateway hard-rejects a policy whose schema it does not know, keeps
        serving from its cached copy, and goes 503 when that expires -- so
        publishing a new schema to a fleet that has not been upgraded is a
        delayed, silent, fleet-wide data-plane outage, arriving up to seven days
        after the publish that caused it.

        max_schema lets a gateway say what it can verify, and is optional so an
        older gateway that does not send it keeps the previous behaviour rather
        than breaking on the day this ships. A tenant can then carry a mixed
        fleet through a rolling upgrade: publish schema 2, gateways that
        understand it take it, gateways that do not keep taking the newest
        schema 1 until they are replaced.
        """
        db, p = s
        if max_schema is not None:
            row = db.execute(
                "SELECT envelope FROM policies WHERE tenant_id=%s AND schema<=%s"
                " ORDER BY version DESC LIMIT 1",
                (p["tenant_id"], max_schema),
            ).fetchone()
        else:
            row = db.execute("SELECT envelope FROM policies WHERE tenant_id=%s ORDER BY version DESC LIMIT 1", (p["tenant_id"],)).fetchone()
        if row is None:
            raise HTTPException(404, "no policy")
        return row["envelope"]

    @app.put("/v1/policy")
    def publish(body: dict, request: Request, s=Depends(session, scope="function")):
        db, p = s
        require(p, "admin", "publisher")
        try:
            validate_policy(body, p["tenant_id"])
        except (ValueError, TypeError, KeyError):
            raise HTTPException(422, "invalid policy")
        # Serializes first publication and subsequent versions per tenant.
        db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (p["tenant_id"],))
        row = db.execute("SELECT max(version) AS version FROM policies WHERE tenant_id=%s", (p["tenant_id"],)).fetchone()
        if row["version"] is not None and body["version"] <= row["version"]:
            raise HTTPException(409, "version must increase")
        envelope = sign(body, request.app.state.key_id, request.app.state.seed)
        # schema denormalised from the document the signature covers, so a
        # gateway can ask for the newest policy it is able to verify without
        # anything having to decode a signed payload in a query.
        db.execute("INSERT INTO policies(tenant_id,version,envelope,schema) VALUES(%s,%s,%s,%s)",
                   (p["tenant_id"], body["version"], Jsonb(envelope), body["schema"]))
        audit(db, p, "policy.publish", {"version": body["version"], "key_id": envelope["key_id"]})
        return envelope

    @app.post("/v1/telemetry")
    def telemetry(body: Event, s=Depends(session, scope="function")):
        db, p = s
        require(p, "agent")
        if body.end_ns < body.start_ns or body.end_ns > time.time_ns() + 60_000_000_000:
            raise HTTPException(422, "invalid event timestamps")
        db.execute("INSERT INTO telemetry(tenant_id,id,event) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
                   (p["tenant_id"], body.id, Jsonb(body.model_dump(exclude_none=True))))
        return {"id": body.id}

    # Additive, deliberately. The per-event route above stays, so a gateway that
    # predates batching keeps working and a gateway that postdates an old control
    # plane falls back to it on 404. Either component can be deployed first, which
    # is what makes this safe to ship independently.
    @app.get("/v1/savings")
    def savings(hours: int = 24, s=Depends(session, scope="function")):
        """What routing cost and what it avoided, over a window.

        Tokens, never money. This service has no price table and inventing one
        would put a fabricated number in front of whoever reads it; rates are per
        contract and change. The caller multiplies by their own rate, which is
        also why the figures below are separated by kind rather than summed.

        The viewer role can read this. It is the same set that can read
        /v1/telemetry, because this is that data aggregated and nothing more --
        a reader who can see the events can already compute this by hand.
        """
        db, p = s
        require(p, "admin", "publisher", "viewer")
        # Bounded, because this aggregates a table that grows with traffic and an
        # unbounded window is a slow query somebody will eventually issue.
        hours = max(1, min(hours, 24 * 31))

        # One pass. Every figure comes from the same rows, so they cannot
        # disagree about which requests were in the window.
        rows = db.execute(
            """
            SELECT
              coalesce(event->>'provider','')                              AS provider,
              coalesce(event->'ext'->>'model','')                          AS model,
              count(*)                                                     AS requests,
              sum((event->'ext'->>'gen_ai.usage.input_tokens')::bigint)    AS input_tokens,
              sum((event->'ext'->>'gen_ai.usage.output_tokens')::bigint)   AS output_tokens,
              sum((event->'ext'->>'gen_ai.usage.cache_read.input_tokens')::bigint)  AS cache_read,
              sum((event->'ext'->>'gen_ai.usage.cache_write.input_tokens')::bigint) AS cache_write,
              count(*) FILTER (WHERE (event->>'attempts')::int > 1)        AS failed_over,
              count(*) FILTER (WHERE event->'ext'->>'idempotent_replay' = 'true') AS replays,
              count(*) FILTER (WHERE event->'ext' ? 'budget_skipped')      AS budget_skipped,
              -- Routes the loop passed over without calling. The breaker's most
              -- valuable work: a provider call not made is not billed and not
              -- waited on, and it is why a run of failures produces far fewer
              -- failovers than requests.
              coalesce(sum(jsonb_array_length(event->'ext'->'skipped')) FILTER (
                WHERE jsonb_typeof(event->'ext'->'skipped') = 'array'), 0) AS routes_skipped
            FROM telemetry
            WHERE received_at > now() - make_interval(hours => %s)
            GROUP BY 1, 2
            ORDER BY coalesce(sum((event->'ext'->>'gen_ai.usage.input_tokens')::bigint), 0) DESC
            """,
            (hours,)).fetchall()
        routes = [dict(r) for r in rows]

        fault_rows = db.execute(
            """SELECT coalesce(event->'ext'->>'fault','') AS fault, count(*) AS n
                 FROM telemetry
                WHERE received_at > now() - make_interval(hours => %s)
                  AND event->'ext' ? 'fault'
                GROUP BY 1""",
            (hours,)).fetchall()
        faults = {r["fault"]: r["n"] for r in fault_rows}

        total = lambda k: sum(r[k] or 0 for r in routes)
        return {
            "window_hours": hours,
            # Stated so a reader knows whether an empty page means "nothing
            # happened" or "nothing was sent", which are very different.
            "requests": total("requests"),
            "routes": routes,
            "faults": faults,
            "tokens": {
                "input": total("input_tokens"),
                "output": total("output_tokens"),
                # Measured, not estimated: these were billed at the provider's
                # cache rate rather than the input rate, so this is the one
                # saving here that is an observation.
                "cache_read": total("cache_read"),
                "cache_write": total("cache_write"),
            },
            "avoided": {
                "replayed_requests": total("replays"),
                "budget_skipped_requests": total("budget_skipped"),
            },
            "failed_over_requests": total("failed_over"),
            # Counted, never priced. A skipped route is a call that did not
            # happen, and converting that to money would need a rate table this
            # service deliberately does not have.
            "routes_skipped": total("routes_skipped"),
        }

    @app.get("/v1/requests")
    def requests_list(hours: int = 24, limit: int = 2000, s=Depends(session, scope="function")):
        """One row per request, projected to the figures a chart can plot.

        /v1/savings aggregates by route, which answers where the spend went and
        cannot answer which individual requests were unusual. That is a
        different question and needs the rows, not the totals: an average hides
        the request that took nine seconds, and the whole reason to keep
        per-request telemetry is to be able to find it.

        Projected in SQL rather than returning the event blobs. A chart needs
        eight numbers per request and the blob carries far more, most of it
        repeated on every row.

        Same read set as /v1/savings and /v1/telemetry, and the same reasoning:
        this is those events with the columns picked out, so a reader who can
        see them can already compute this by hand.

        No prompts and no completions. Captured content never reaches the
        control plane at all -- it stays on the gateway's disk -- and nothing
        here would change that, but the projection names its columns explicitly
        rather than selecting whatever the event happens to hold, so a new field
        on the gateway cannot arrive here unnoticed.
        """
        db, p = s
        require(p, "admin", "publisher", "viewer")
        # Both bounded, for the reason savings() states: this reads a table that
        # grows with traffic, and an unbounded query is one somebody eventually
        # issues by accident.
        hours = max(1, min(hours, 24 * 31))
        limit = max(1, min(limit, 5000))

        rows = db.execute(
            """
            SELECT
              extract(epoch from received_at)                               AS at,
              coalesce(event->>'provider','')                               AS provider,
              coalesce(event->'ext'->>'model','')                           AS model,
              (event->>'status')::int                                       AS status,
              (event->>'attempts')::int                                     AS attempts,
              ((event->>'end_ns')::bigint - (event->>'start_ns')::bigint)/1000000
                                                                            AS duration_ms,
              -- Not coalesced to 0, deliberately. A request refused before
              -- routing carries no token counts at all, and 0 is a different
              -- claim from absent: it says a provider was asked and charged
              -- nothing. /v1/savings already leaves these NULL, via sum() over
              -- no rows, so coalescing here gave the same data two meanings
              -- depending on which endpoint you read it from.
              --
              -- It is not only a naming quibble. Average the input tokens over
              -- these rows with unrouted requests contributing 0 and the answer
              -- is wrong; with NULL they are excluded, which is what an average
              -- of "tokens per request that reached a provider" means. The
              -- page's own rule is that a blank is silence, never a zero.
              (event->'ext'->>'gen_ai.usage.input_tokens')::bigint       AS input_tokens,
              (event->'ext'->>'gen_ai.usage.output_tokens')::bigint      AS output_tokens,
              (event->'ext'->>'gen_ai.usage.cache_read.input_tokens')::bigint
                                                                            AS cache_read,
              coalesce(event->'ext'->>'fault','')                           AS fault,
              coalesce(event->'ext'->>'requested_model','')                  AS requested_model
            FROM telemetry
            WHERE received_at > now() - make_interval(hours => %s)
            ORDER BY received_at DESC
            LIMIT %s
            """,
            (hours, limit)).fetchall()

        return {
            "window_hours": hours,
            "limit": limit,
            # So a page can say it is showing a sample rather than implying it
            # is showing the population. A chart that silently truncates is a
            # chart that lies by omission.
            "truncated": len(rows) >= limit,
            "rows": [dict(r) for r in rows],
        }

    @app.get("/v1/requests/{request_id}")
    def request_detail(request_id: str, s=Depends(session, scope="function")):
        """One request in full: every attempt, and every route passed over.

        A second endpoint rather than a column on /v1/requests. That one is a
        wide scan projected to eight numbers per row and is read to draw charts;
        attaching a JSON array of attempts to every row would bloat the common
        path in order to serve the rare one.

        What it adds is the per-attempt record. /v1/requests says a request made
        two attempts and what it cost in total; this says the first attempt went
        to one provider, returned 200 with no text, and was billed 1024 tokens
        for it. That is the difference between knowing a failover happened and
        knowing what it cost.

        Same read set and the same reasoning as its neighbours: this is one row
        of the events a viewer can already read. No prompts and no completions --
        captured content never reaches the control plane at all.
        """
        db, p = s
        require(p, "admin", "publisher", "viewer")
        # The Event model pins request_id to 32 hex characters, so this checks
        # the same shape rather than the looser identifier pattern. The query is
        # parameterised either way; this is about rejecting a malformed lookup
        # with 400 instead of scanning for something that cannot exist.
        if not re.fullmatch(r"[0-9a-f]{32}", request_id):
            raise HTTPException(400, "request_id must be 32 hex characters")

        row = db.execute(
            """SELECT event, received_at FROM telemetry
                WHERE event->>'request_id' = %s
                ORDER BY received_at DESC LIMIT 1""",
            (request_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "no such request in this tenant's telemetry")

        e = row["event"]
        ext = e.get("ext") or {}
        return {
            "request_id": e.get("request_id"),
            "trace_id": e.get("trace_id"),
            "received_at": row["received_at"],
            "status": e.get("status"),
            "attempts": e.get("attempts"),
            "provider": e.get("provider"),
            "model": ext.get("model"),
            "requested_model": ext.get("requested_model"),
            "policy_version": e.get("policy_version"),
            "duration_ms": (e.get("end_ns", 0) - e.get("start_ns", 0)) // 1_000_000,
            "fault": ext.get("fault"),
            # The per-attempt record, each with its own token usage. Absent on a
            # request that never reached a provider, which is a fact rather than
            # a gap -- the request was answered without calling anyone.
            "tries": ext.get("tries", []),
            # Routes passed over without being called, with the reason.
            "skipped": ext.get("skipped", []),
        }

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard():
        """The savings dashboard, served from the API's own origin.

        Same origin on purpose. Every read here authenticates with a bearer
        token, and a page served from anywhere else turns each call into a CORS
        preflight -- which ends with a permissive CORS policy bolted onto an
        authenticated API, to make a convenience work.

        The document carries no data and no credential. It asks the reader for
        their own token, keeps it in sessionStorage and sends it as a header, so
        there is no cookie and therefore no CSRF surface. Unauthenticated
        because it is a shell: everything of value behind it still needs the
        token.
        """
        # Read per request rather than cached at import, so editing the page
        # during local development does not need a restart. It is one small
        # file off local disk on a route nobody polls.
        return HTMLResponse((pathlib.Path(__file__).parent / "dashboard.html").read_text())

    @app.post("/v1/health")
    def health_sync(body: HealthSync, s=Depends(session, scope="function")):
        """Exchange circuit state with the rest of this tenant's fleet.

        Deliberately not part of GET /v1/policy. That path is the trust anchor:
        it serves a signed document a gateway will verify and act on, and
        nothing about advisory health state should be able to fail, slow or
        confuse it. Same ticker, separate endpoint.

        The response is advisory in the strict sense -- a gateway that never
        calls this, or calls it and gets an error, behaves exactly as it did
        before this endpoint existed. That property is the reason the feature is
        safe to ship, so it is worth not trading away later for a faster
        propagation story.
        """
        db, p = s
        require(p, "agent")

        for r in body.reports:
            # One row per instance per provider, overwritten each poll. A report
            # is a current statement, not an event: keeping the history would be
            # a retention problem in exchange for nothing.
            db.execute(
                """INSERT INTO provider_health(tenant_id,instance_id,provider,fault,reported_at,expires_at)
                   VALUES(%s,%s,%s,%s,now(),now() + make_interval(secs => %s))
                   ON CONFLICT (tenant_id,instance_id,provider)
                   DO UPDATE SET fault=EXCLUDED.fault, reported_at=now(), expires_at=EXCLUDED.expires_at""",
                (p["tenant_id"], body.instance_id, r.provider, r.fault, r.ttl_seconds))

        # Swept on read rather than on a schedule. The table is small, the read
        # already touches the tenant's rows, and a sweep nobody runs is how the
        # idempotency store ended up holding a week of entries under a one-day
        # TTL.
        db.execute("DELETE FROM provider_health WHERE expires_at <= now()")

        db.execute(
            """SELECT provider, fault, count(DISTINCT instance_id) AS instances,
                      max(expires_at) AS until
                 FROM provider_health
                WHERE expires_at > now()
                GROUP BY provider, fault""")
        unhealthy = []
        for row in db.fetchall():
            # An account fault is unambiguous: the provider said this tenant
            # cannot pay, and one instance hearing it is enough. A degraded
            # fault is a 5xx, which might be one host's network, so it needs a
            # second instance to agree before the fleet acts on it.
            if row["fault"] == "degraded" and row["instances"] < 2:
                continue
            unhealthy.append({
                "provider": row["provider"],
                "fault": row["fault"],
                "instances": row["instances"],
                "seconds": max(1, int((row["until"] - datetime.now(timezone.utc)).total_seconds())),
            })
        return {"unhealthy": unhealthy}

    @app.post("/v1/telemetry/batch")
    def telemetry_batch(body: EventBatch, s=Depends(session, scope="function")):
        db, p = s
        require(p, "agent")
        accepted = []
        for raw in body.events:
            # Per element, so a malformed one costs itself and not the batch.
            try:
                e = Event.model_validate(raw)
            except ValidationError:
                continue
            if e.end_ns < e.start_ns or e.end_ns > time.time_ns() + 60_000_000_000:
                continue
            db.execute("INSERT INTO telemetry(tenant_id,id,event) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
                       (p["tenant_id"], e.id, Jsonb(e.model_dump(exclude_none=True))))
            accepted.append(e.id)
        # The ids, not a count: the sender deletes exactly these, so a partially
        # applied batch converges instead of losing events or resending forever.
        return {"accepted": accepted}

    @app.get("/v1/telemetry")
    def telemetry_list(s=Depends(session, scope="function")):
        db, p = s
        require(p, "admin", "publisher", "viewer")
        return db.execute("SELECT event,received_at FROM telemetry WHERE tenant_id=%s ORDER BY received_at DESC LIMIT 100", (p["tenant_id"],)).fetchall()

    @app.get("/v1/replay/{request_id}")
    def replay(request_id: str, s=Depends(session, scope="function")):
        """Which policy routed this request, and whether the provider was allowed.

        The routing decision has always been reconstructible -- every signed
        envelope is kept by (tenant, version) forever -- but nothing recorded
        which version was live until the gateway started sending policy_version.
        With it, this is a join rather than a service.

        The tenant comes from the authenticated principal and is never a
        parameter. Every other handler here establishes that, and replay is
        exactly the endpoint where accepting one would be tempting and wrong.

        No captured content is served. Prompts and completions, when capture is
        enabled at all, live on the gateway's own disk and never reach the
        control plane; an endpoint that returned them would undo the property
        that keeps that exposure to one machine. Read them with
        `python -m controlplane.replay --capture-dir` on the box that served the
        request.

        Reuses replay.py rather than reimplementing the reconstruction, so the
        CLI and the endpoint cannot drift into disagreeing about what happened.
        """
        db, p = s
        require(p, "admin", "publisher", "viewer")
        if not re.fullmatch(r"[0-9a-f]{32}", request_id):
            raise HTTPException(404, "no such request")

        row = db.execute(
            "SELECT event,received_at FROM telemetry"
            " WHERE tenant_id=%s AND event->>'request_id'=%s"
            " ORDER BY received_at DESC LIMIT 1",
            (p["tenant_id"], request_id),
        ).fetchone()
        if row is None:
            # Telemetry is delivered asynchronously and dropped rather than
            # retried forever, so absence is not proof the request never
            # happened -- say so rather than implying it.
            raise HTTPException(404, "no event for that request id; telemetry is "
                                     "delivered asynchronously and may not have arrived")

        version = (row["event"] or {}).get("policy_version")
        policy = None
        if version:
            policy = db.execute(
                "SELECT version,envelope,created_at FROM policies WHERE tenant_id=%s AND version=%s",
                (p["tenant_id"], version),
            ).fetchone()
        # One implementation, shared with the CLI. The endpoint previously
        # re-derived the consistency verdict, the model inference and the
        # undecodable-envelope case, with a separate test suite and nothing
        # comparing the two -- so they could have disagreed about what happened
        # to a request and no test would have caught it.
        return reconstruct(row, policy)

    @app.get("/v1/audit")
    def audit_list(s=Depends(session, scope="function")):
        db, p = s
        require(p, "admin", "viewer")
        return db.execute("SELECT id,action,detail,created_at FROM audit WHERE tenant_id=%s ORDER BY id DESC LIMIT 100", (p["tenant_id"],)).fetchall()

    @app.post("/v1/principals", status_code=201)
    def add(body: NewPrincipal, s=Depends(session, scope="function")):
        db, p = s
        require(p, "admin")
        if not time.time() < body.expires_at <= time.time() + 90*86400:
            raise HTTPException(422, "expiry must be within 90 days")
        pid = uuid.uuid4()
        db.execute("SELECT add_principal(%s,%s,%s,%s)", (pid, body.token_hash, body.role, datetime.fromtimestamp(body.expires_at, timezone.utc)))
        audit(db, p, "principal.create", {"id": str(pid), "role": body.role})
        return {"id": str(pid)}

    @app.delete("/v1/principals/{pid}", status_code=204)
    def revoke(pid: uuid.UUID, s=Depends(session, scope="function")):
        db, p = s
        require(p, "admin")
        db.execute("SELECT revoke_principal(%s)", (pid,))
        audit(db, p, "principal.revoke", {"id": str(pid)})

    return app

app = create_app()
