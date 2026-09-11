"""Integration tests require a disposable Postgres cluster. CI runs these mandatorily."""
import hashlib
import os
import secrets
import time
import uuid
import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from fastapi.testclient import TestClient
from controlplane.app import create_app
from controlplane.migrate import migrate

@pytest.fixture(scope="module")
def setup():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL is required for Postgres integration")
    migrate(dsn)
    migrate(dsn)  # Re-running migrations is a no-op with matching checksums.
    tokens = {}
    with psycopg.connect(dsn, autocommit=True) as db:
        for tenant in ("tenant-a", "tenant-b"):
            db.execute("INSERT INTO tenants(id) VALUES(%s)", (tenant,))
            for role in ("admin", "publisher", "viewer", "agent"):
                token = secrets.token_hex(32)
                tokens[tenant, role] = token
                db.execute("INSERT INTO principals(id,tenant_id,token_hash,role,expires_at) VALUES(%s,%s,%s,%s,now()+interval '1 day')",
                           (uuid.uuid4(), tenant, hashlib.sha256(token.encode()).hexdigest(), role))
    # SET ROLE verifies RLS behavior even when test DSN is a superuser.
    def configure(db):
        db.execute("SET ROLE switchboard_app")
        db.commit()
    pool = ConnectionPool(dsn, min_size=1, max_size=4, configure=configure, kwargs={"row_factory": dict_row})
    with TestClient(create_app(pool=pool, seed=secrets.token_bytes(32), key_id="test")) as client:
        yield client, tokens, pool
    pool.close()

def auth(tokens, tenant="tenant-a", role="admin"):
    return {"Authorization": "Bearer " + tokens[tenant, role]}

def policy(version=1):
    now = int(time.time())
    return {"schema": 1, "tenant": "tenant-a", "version": version, "issued_at": now,
            "expires_at": now+3600, "routes": [{"provider": "openai", "model": "test"}]}

def test_policy_rbac_tenant_and_rollback(setup):
    c,t,_ = setup
    assert c.put("/v1/policy",json=policy(),headers=auth(t,role="viewer")).status_code == 403
    assert c.put("/v1/policy",json=policy(),headers=auth(t)).status_code == 200
    assert c.put("/v1/policy",json=policy(),headers=auth(t)).status_code == 409
    assert c.get("/v1/policy",headers=auth(t,"tenant-b")).status_code == 404
    assert c.put("/v1/policy",json=policy(2),headers=auth(t,"tenant-b")).status_code == 422
    assert c.get("/v1/policy").status_code == 401

def test_telemetry_dedup_and_isolation(setup):
    c,t,pool = setup
    body={"id":uuid.uuid4().hex,"request_id":uuid.uuid4().hex,"trace_id":uuid.uuid4().hex,
          "span_id":"a"*16,"provider":"openai","status":200,"attempts":1,"start_ns":1,"end_ns":2}
    for _ in range(2):
        assert c.post("/v1/telemetry",json=body,headers=auth(t,role="agent")).status_code == 200
    assert c.post("/v1/telemetry",json=body,headers=auth(t,role="viewer")).status_code == 403
    assert len(c.get("/v1/telemetry",headers=auth(t)).json()) == 1
    assert c.get("/v1/telemetry",headers=auth(t,"tenant-b")).json() == []
    with pool.connection() as db:
        assert db.execute("SELECT count(*) AS n FROM telemetry").fetchone()["n"] == 0

def _event(**over):
    e = {"id":uuid.uuid4().hex,"request_id":uuid.uuid4().hex,"trace_id":uuid.uuid4().hex,
         "span_id":"a"*16,"provider":"openai","status":200,"attempts":1,"start_ns":1,"end_ns":2}
    e.update(over)
    return e

def test_telemetry_batch(setup):
    c,t,_ = setup
    events = [_event() for _ in range(3)]
    r = c.post("/v1/telemetry/batch",json={"events":events},headers=auth(t,role="agent"))
    assert r.status_code == 200
    # The ids, not a count: the sender deletes exactly what came back, so a
    # partially applied batch converges rather than losing or resending forever.
    assert r.json()["accepted"] == [e["id"] for e in events]

    # Idempotent on the event id, which is what makes a retried batch safe: the
    # ids come back again, and no duplicate rows appear.
    again = c.post("/v1/telemetry/batch",json={"events":events},headers=auth(t,role="agent"))
    assert again.status_code == 200 and again.json()["accepted"] == [e["id"] for e in events]
    stored = c.get("/v1/telemetry",headers=auth(t)).json()
    assert len([r for r in stored if r["event"]["id"] in {e["id"] for e in events}]) == len(events)
    # Written under the other tenant's identity, they are invisible to it.
    assert c.get("/v1/telemetry",headers=auth(t,"tenant-b")).json() == []

def test_telemetry_batch_skips_bad_events_without_failing_the_batch(setup):
    c,t,_ = setup
    good, bad = _event(), _event(end_ns=0)   # end_ns < start_ns
    r = c.post("/v1/telemetry/batch",json={"events":[good,bad]},headers=auth(t,role="agent"))
    # One malformed event must not fail the batch. The sender retries whatever was
    # not acknowledged, so a rejected event would otherwise be resent every tick
    # forever and wedge every event behind it.
    assert r.status_code == 200
    assert r.json()["accepted"] == [good["id"]]

def test_telemetry_batch_is_bounded_and_role_gated(setup):
    c,t,_ = setup
    assert c.post("/v1/telemetry/batch",json={"events":[_event()]},headers=auth(t,role="viewer")).status_code == 403
    assert c.post("/v1/telemetry/batch",json={"events":[_event()]}).status_code == 401
    # Unbounded arrays are a denial-of-service vector against a route that inserts
    # every element, so the limit is enforced by the model rather than by hand.
    assert c.post("/v1/telemetry/batch",json={"events":[]},headers=auth(t,role="agent")).status_code == 422
    too_many = [_event() for _ in range(201)]
    assert c.post("/v1/telemetry/batch",json={"events":too_many},headers=auth(t,role="agent")).status_code == 422

def test_revocation_and_audit(setup):
    c,t,_=setup
    token=secrets.token_hex(32)
    r=c.post("/v1/principals",json={"token_hash":hashlib.sha256(token.encode()).hexdigest(),"role":"viewer","expires_at":int(time.time())+300},headers=auth(t))
    assert r.status_code==201
    h={"Authorization":"Bearer "+token}
    assert c.get("/v1/telemetry",headers=h).status_code==200
    assert c.delete("/v1/principals/"+r.json()["id"],headers=auth(t)).status_code==204
    assert c.get("/v1/telemetry",headers=h).status_code==401
    assert len(c.get("/v1/audit",headers=auth(t)).json())>=2

def test_replay_reconstructs_the_routing_decision(setup):
    """The point of recording policy_version: turn a request id into an account
    of where it went and why, from records the control plane already keeps."""
    c,t,_ = setup
    # Publish a policy so there is an envelope to join back to.
    doc = policy(version=7)
    doc["routes"] = [{"provider":"openai","model":"gpt-5-nano"},
                     {"provider":"anthropic","model":"claude-haiku-4-5-20251001"}]
    assert c.put("/v1/policy",json=doc,headers=auth(t)).status_code == 200

    ev = _event(provider="anthropic", policy_version=7)
    assert c.post("/v1/telemetry/batch",json={"events":[ev]},headers=auth(t,role="agent")).status_code == 200

    r = c.get(f"/v1/replay/{ev['request_id']}",headers=auth(t))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["policy_version"] == 7
    assert body["provider"] == "anthropic"
    # The route order, and that the provider which answered was one this policy
    # allowed. A provider outside the list means the policy rotated mid-flight
    # or something is wrong, and silence there would make the answer misleading.
    assert [x["provider"] for x in body["routes"]] == ["openai","anthropic"]
    assert body["consistent"] is True
    # A policy forbids a repeated provider, so the model follows from the
    # provider and never had to be stored on the event.
    assert body["model"] == "claude-haiku-4-5-20251001"

def test_replay_is_tenant_scoped_and_never_takes_a_tenant_parameter(setup):
    """The tenant comes from the authenticated principal. Replay is exactly the
    endpoint where accepting one from the caller would be tempting and wrong."""
    c,t,_ = setup
    ev = _event()
    assert c.post("/v1/telemetry/batch",json={"events":[ev]},headers=auth(t,role="agent")).status_code == 200
    assert c.get(f"/v1/replay/{ev['request_id']}",headers=auth(t)).status_code == 200
    # Same request id, different tenant's credentials: it does not exist.
    assert c.get(f"/v1/replay/{ev['request_id']}",headers=auth(t,"tenant-b")).status_code == 404

def test_replay_absence_does_not_claim_the_request_never_happened(setup):
    """Telemetry is delivered asynchronously and dropped rather than retried
    forever, so a missing event is not evidence of a missing request."""
    c,t,_ = setup
    r = c.get("/v1/replay/" + "0"*32, headers=auth(t))
    assert r.status_code == 404
    assert "asynchronously" in r.json()["detail"]

def test_replay_rejects_ids_that_are_not_request_ids(setup):
    c,t,_ = setup
    for bad in ["../../etc/passwd", "short", "A"*32, "%00"*8]:
        assert c.get(f"/v1/replay/{bad}",headers=auth(t)).status_code in (404,)

def test_replay_requires_authentication(setup):
    c,t,_ = setup
    assert c.get("/v1/replay/" + "0"*32).status_code == 401

def test_event_ext_survives_and_core_stays_strict(setup):
    """The forward-compatibility contract, both halves of it.

    A field the control plane does not know must arrive as data rather than as
    a validation failure -- adding policy_version made every event from a newer
    gateway fail on an older control plane, and the gateway drops what is not
    acknowledged, so the symptom was silence. But the core must stay strict: a
    typo in request_id should be a rejected event, not a silently mis-stored one.
    """
    c,t,_ = setup
    ev = _event(ext={"model":"claude-haiku-4-5-20251001","fault":"rate_limit",
                     "something_invented_later": 42})
    r = c.post("/v1/telemetry/batch",json={"events":[ev]},headers=auth(t,role="agent"))
    assert r.status_code == 200 and r.json()["accepted"] == [ev["id"]]

    stored = [x for x in c.get("/v1/telemetry",headers=auth(t)).json()
              if x["event"]["id"] == ev["id"]][0]["event"]
    # Verbatim, including the key nobody has written a model for.
    assert stored["ext"]["model"] == "claude-haiku-4-5-20251001"
    assert stored["ext"]["fault"] == "rate_limit"
    assert stored["ext"]["something_invented_later"] == 42

    # The core is still closed: an unknown TOP-LEVEL key is a rejection, which
    # is what keeps a typo from becoming a quietly wrong record.
    bad = _event(); bad["requst_id"] = bad.pop("request_id")
    r2 = c.post("/v1/telemetry/batch",json={"events":[bad]},headers=auth(t,role="agent"))
    assert r2.status_code == 200 and r2.json()["accepted"] == []

def test_attempts_is_not_bounded_by_todays_retry_ceiling(setup):
    """max_attempts is validated 1..3 in the gateway's own config, but that is a
    local operating choice. Encoding it on the wire means raising it later
    rejects every event from a gateway that does."""
    c,t,_ = setup
    ev = _event(attempts=7)
    r = c.post("/v1/telemetry/batch",json={"events":[ev]},headers=auth(t,role="agent"))
    assert r.json()["accepted"] == [ev["id"]], "a wire bound encoded a local policy"

def test_policy_schema_negotiation(setup):
    """A gateway hard-rejects a policy schema it cannot verify, keeps serving its
    cached copy, and goes 503 when that expires -- so publishing a new schema to
    a fleet that has not been upgraded is a delayed, silent, fleet-wide outage
    arriving up to seven days later. max_schema turns that into a rolling
    upgrade."""
    c,t,_ = setup
    assert c.put("/v1/policy",json=policy(version=900),headers=auth(t)).status_code == 200

    # A gateway that says what it can verify gets it.
    r = c.get("/v1/policy?max_schema=1",headers=auth(t,role="agent"))
    assert r.status_code == 200

    # Omitting the parameter keeps the old behaviour, so a gateway predating
    # this change does not break on the day it ships.
    assert c.get("/v1/policy",headers=auth(t,role="agent")).status_code == 200

    # A schema this caller cannot verify is not served to it. Simulated by
    # asking for a lower ceiling than anything published.
    assert c.get("/v1/policy?max_schema=0",headers=auth(t,role="agent")).status_code == 404

def test_policy_schema_is_recorded_for_negotiation(setup):
    """Denormalised at publish time, because the writer already parsed the
    document and decoding a signed payload in a query to recover a number it
    had is the wrong place to spend it."""
    c,t,pool = setup
    assert c.put("/v1/policy",json=policy(version=901),headers=auth(t)).status_code == 200
    with pool.connection() as db:
        db.execute("SELECT set_config('app.tenant','tenant-a',false)")
        row = db.execute("SELECT schema FROM policies WHERE tenant_id='tenant-a'"
                         " ORDER BY version DESC LIMIT 1").fetchone()
    # dict_row: the pool returns mappings, not tuples.
    assert row["schema"] == 1


def _dig(doc, path):
    for k in path:
        doc = doc[k]
    return doc or 0


def _usage_event(rid, provider, attempts, ext):
    """One telemetry event in the shape the gateway actually sends."""
    now = time.time_ns()
    return {"id": secrets.token_hex(16), "request_id": rid, "trace_id": secrets.token_hex(16),
            "span_id": secrets.token_hex(8), "provider": provider, "status": 200,
            "attempts": attempts, "start_ns": now, "end_ns": now + 1, "ext": ext}


def test_savings_totals_are_the_arithmetic_they_claim(setup):
    """The arithmetic is the product, so it is asserted rather than eyeballed.

    A dashboard that quietly sums the wrong column still draws a confident
    number, and the person reading it has no way to tell.
    """
    client, tokens, _ = setup
    events = [
        _usage_event(secrets.token_hex(16), "anthropic", 1, {
            "model": "claude-haiku-4-5",
            "gen_ai.usage.input_tokens": 1000,
            "gen_ai.usage.output_tokens": 100,
            "gen_ai.usage.cache_read.input_tokens": 800,
        }),
        _usage_event(secrets.token_hex(16), "anthropic", 2, {
            "model": "claude-haiku-4-5",
            "gen_ai.usage.input_tokens": 500,
            "gen_ai.usage.output_tokens": 50,
            "fault": "rate_limit",
        }),
        # A replay: served from the store, so no provider was paid.
        _usage_event(secrets.token_hex(16), "", 0, {"idempotent_replay": True}),
        # A request that passed over a route it would otherwise have paid for.
        _usage_event(secrets.token_hex(16), "openai", 1, {
            "model": "gpt-5-nano",
            "gen_ai.usage.input_tokens": 200,
            "gen_ai.usage.output_tokens": 20,
            "budget_skipped": ["openai:gpt-5-reasoning"],
        }),
    ]
    # Deltas, not totals. Every test in this module shares one database and
    # several of them post telemetry, so an absolute assertion here would be
    # asserting on test execution order.
    def read():
        return client.get("/v1/savings?hours=1", headers=auth(tokens, role="viewer")).json()

    before = read()
    r = client.post("/v1/telemetry/batch", json={"events": events},
                    headers=auth(tokens, role="agent"))
    assert r.status_code == 200, r.text
    got = read()

    d = lambda path: _dig(got, path) - _dig(before, path)
    assert d(("tokens", "input")) == 1700, (before, got)       # 1000 + 500 + 200
    assert d(("tokens", "output")) == 170, (before, got)       # 100 + 50 + 20
    assert d(("tokens", "cache_read")) == 800, (before, got)   # measured, not estimated
    assert d(("avoided", "replayed_requests")) == 1, (before, got)
    assert d(("avoided", "budget_skipped_requests")) == 1, (before, got)
    assert d(("failed_over_requests",)) == 1, (before, got)    # the one with attempts 2
    assert got["faults"].get("rate_limit", 0) - before["faults"].get("rate_limit", 0) == 1, (before, got)
    # Never money: this service has no price table and must not invent one.
    assert "cost" not in got and "usd" not in str(got).lower(), got


def test_savings_is_tenant_isolated(setup):
    """An aggregate endpoint is exactly where a missing tenant predicate hides.

    A per-row read that leaks is obvious in the response. A sum that includes
    another tenant's traffic looks like a plausible number.
    """
    client, tokens, _ = setup
    r = client.post("/v1/telemetry/batch", json={"events": [
        _usage_event(secrets.token_hex(16), "gemini", 1, {
            "model": "gemini-3.6-flash", "gen_ai.usage.input_tokens": 999_999,
            "gen_ai.usage.output_tokens": 1})]},
        headers=auth(tokens, tenant="tenant-b", role="agent"))
    assert r.status_code == 200, r.text

    a = client.get("/v1/savings?hours=1", headers=auth(tokens, tenant="tenant-a", role="viewer")).json()
    assert a["tokens"]["input"] != 999_999 and a["tokens"]["input"] < 999_999, a
    assert all(r["provider"] != "gemini" for r in a["routes"]), a

    b = client.get("/v1/savings?hours=1", headers=auth(tokens, tenant="tenant-b", role="viewer")).json()
    assert b["tokens"]["input"] == 999_999, b


def test_savings_window_is_bounded_and_role_gated(setup):
    client, tokens, _ = setup
    # An unbounded window is a slow query somebody will eventually issue.
    assert client.get("/v1/savings?hours=99999", headers=auth(tokens, role="viewer")).json()["window_hours"] == 24 * 31
    assert client.get("/v1/savings?hours=0", headers=auth(tokens, role="viewer")).json()["window_hours"] == 1
    # agent writes telemetry; it does not get to read the spend aggregate.
    assert client.get("/v1/savings", headers=auth(tokens, role="agent")).status_code == 403
