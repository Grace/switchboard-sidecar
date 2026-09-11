"""What is worth pinning in honeycombtool: the query shapes, and re-run safety.

Nothing here touches the network. The parts that talk to Honeycomb are thin
wrappers around urllib and would only be testing a mock of the API; the parts
that are easy to get silently wrong are the query specs, and getting one wrong
produces a trigger that is accepted and never fires.
"""
import pytest

from controlplane import alerting, honeycomb
from controlplane.honeycomb import (
    NEEDED,
    NOTIFY_TRIGGER,
    PAGE_TRIGGER,
    board_queries,
    combined,
    counter,
    find_by_name,
    main,
    triggers,
)


def calc_ops(query):
    return {c["op"] for c in query["calculations"]}


def test_counters_aggregate_with_sum_never_rate_sum():
    """RATE_SUM is refused on a Metrics dataset, and the refusal is invisible.

    Honeycomb stores a trigger holding one, lists it as healthy, and never
    evaluates it, because the query it would run is one the engine rejects. Both
    triggers in the first account this provisioned were in that state. SUM is
    correct as well as accepted: the counter's own temporal aggregation is
    applied first, so SUM over the window is the increase during it.
    """
    for spec in triggers():
        ops = calc_ops(spec["query"])
        assert ops == {"SUM"}, f"{spec['name']} aggregates with {ops}"
    for _, _, q in board_queries():
        assert "RATE_SUM" not in calc_ops(q)


def test_notify_trigger_reduces_three_counters_to_one_value():
    """A trigger query may hold only one aggregate; a formula is how three fit.

    Without the formula the API refuses the query outright with "only one
    non-having aggregate is allowed", which is what makes this the mechanism
    that fits four alertable conditions into the free plan's two triggers.
    """
    q = combined(alerting.by_urgency(alerting.NOTIFY), 900)
    notify = alerting.by_urgency(alerting.NOTIFY)
    assert len(q["calculations"]) == len(notify) == 3
    assert len(q["formulas"]) == 1
    assert q["formulas"][0]["expression"] == " + ".join("$" + c.key for c in notify)
    # Every alias the formula names has to exist as a calculation, or the
    # expression references nothing and the threshold compares against nothing.
    names = {c["name"] for c in q["calculations"]}
    for c in notify:
        assert c.key in names


def test_page_trigger_watches_only_lost_service():
    """The page stays alone deliberately.

    It is the only one of the four conditions that is an outage. Folding another
    counter in would mean the alert that should wake someone also fires for a
    billing problem, and an alert that cries wolf stops being a page.
    """
    page = next(t for t in triggers() if t["name"] == PAGE_TRIGGER)
    assert len(page["query"]["calculations"]) == 1
    assert "formulas" not in page["query"]
    assert page["query"]["calculations"][0]["column"] == "switchboard.empty_completion_failed_total"
    assert dict(page["tags"][1])["value"] == alerting.PAGE


def test_every_trigger_fires_above_zero():
    """These counters only move when something happened worth a person's time."""
    for spec in triggers():
        assert spec["threshold"] == {"op": ">", "value": 0}


def test_trigger_window_covers_its_evaluation_interval():
    """A window shorter than the frequency leaves gaps nothing ever looks at."""
    for spec in triggers():
        assert spec["query"]["time_range"] >= spec["frequency"]


def test_counter_helper_applies_the_otlp_prefix():
    """The bare name goes in, the dotted name comes out. alerting.py stores
    neither spelling, so this is where the OTLP one is added."""
    q = counter("x_total", "x", 300)
    assert q["calculations"] == [{"column": "switchboard.x_total", "op": "SUM", "name": "x"}]
    assert q["time_range"] == 300


def test_find_by_name_is_exact():
    """Re-run safety rests entirely on this, and a near-match would be worse
    than no match: it would silently rewrite a different trigger."""
    items = [{"name": "alpha", "id": "1"}, {"name": "alpha beta", "id": "2"}]
    assert find_by_name(items, "alpha")["id"] == "1"
    assert find_by_name(items, "alpha beta")["id"] == "2"
    assert find_by_name(items, "alph") is None
    assert find_by_name(items, "Alpha") is None
    assert find_by_name([], "alpha") is None
    # The API returns null rather than [] for an empty collection.
    assert find_by_name(None, "alpha") is None


def test_both_triggers_have_distinct_names():
    """Names are the identity used for updates, so a collision would make one
    trigger overwrite the other on every run."""
    names = [t["name"] for t in triggers()]
    assert len(set(names)) == len(names)
    assert set(names) == {PAGE_TRIGGER, NOTIFY_TRIGGER}


def test_descriptions_carry_the_counter_names():
    """The description is what arrives in the notification. The combined trigger
    cannot say which counter moved, so the text has to name all three or the
    alert is unactionable on its own."""
    notify = next(t for t in triggers() if t["name"] == NOTIFY_TRIGGER)
    for c in alerting.by_urgency(alerting.NOTIFY):
        assert c.metric in notify["description"]


def test_board_panels_are_described():
    """A panel with no description is a chart someone has to reverse-engineer
    while an alert is firing."""
    panels = board_queries()
    assert len(panels) >= 5
    for name, desc, q in panels:
        assert name and len(desc) > 40
        assert q["calculations"]


def test_every_needed_permission_is_explained():
    """The message a deployer gets has to name the switch to flip.

    The first key handed to this tool was a configuration key with two of these
    four switched off, and the tool's answer at the time was to guess it was an
    ingest key. /1/auth had the facts. Each permission therefore carries its own
    reason, so the failure says which one is missing and what it is for rather
    than restating that something is not allowed.
    """
    assert set(NEEDED) == {"triggers", "boards", "recipients", "queries"}
    for perm, why in NEEDED.items():
        assert why and not why.endswith("."), f"{perm} needs a reason phrase"


def test_config_key_is_read_from_its_own_variable(monkeypatch, capsys):
    """No fallback to HONEYCOMB_API_KEY, and the reason is not stylistic.

    That variable holds the ingest key, and the dev stack hands .dev/env to the
    gateway container wholesale -- so a configuration key placed there is
    readable by the data plane, which could then rewrite or delete the alerting
    that watches it. A fallback would make putting it in the wrong variable
    work, which is precisely how it would end up there.
    """
    monkeypatch.setenv("HONEYCOMB_API_KEY", "an-ingest-key")
    monkeypatch.delenv("HONEYCOMB_CONFIG_KEY", raising=False)
    with pytest.raises(SystemExit) as e:
        main(["--dataset", "Metrics", "--no-recipient"])
    msg = str(e.value)
    assert "HONEYCOMB_CONFIG_KEY" in msg
    assert "HONEYCOMB_API_KEY" in msg, "the message must say which variable NOT to use"


def test_recipient_choice_must_be_explicit(capsys):
    """Neither notifying nor not-notifying is a default.

    Both triggers once evaluated correctly and notified nobody for a session,
    which reads as coverage on a dashboard while being none. Silence is
    available but has to be typed.
    """
    with pytest.raises(SystemExit):
        main(["--dataset", "Metrics"])


class FakeAPI:
    """Records every call so a test can assert what a dry run did NOT do."""

    def __init__(self, access=None, triggers=(), boards=(), recipients=()):
        self.calls = []
        self.access = access or {"triggers": True, "boards": True,
                                 "recipients": True, "queries": True}
        self.triggers = list(triggers)
        self.boards = list(boards)
        # Defaulted empty for the same reason it always was, but now settable:
        # every test ran against an account with no recipients, so the lookup that
        # matches an existing one was never exercised and a real bug lived behind
        # it for as long as the tool has existed.
        self.recipients = list(recipients)

    def __call__(self, key, method, path, body=None):
        self.calls.append((method, path))
        if path == "/1/auth":
            return {"type": "configuration", "team": {"slug": "t"},
                    "environment": {"slug": "e"}, "api_key_access": self.access}
        if path == "/1/recipients":
            return self.recipients if method == "GET" else {"id": "r1"}
        if path.startswith("/1/triggers"):
            return self.triggers if method == "GET" else {"id": "t1", "query_id": "q1"}
        if path == "/1/boards":
            return self.boards if method == "GET" else {"id": "b1", "links": {"board_url": "u"}}
        if path.startswith("/1/queries"):
            return {"id": "q1"}
        if path.startswith("/1/query_annotations"):
            return {"id": "a1"}
        if path.startswith("/1/query_results"):
            return {"id": "qr1", "complete": True}
        return {}

    def writes(self):
        return [(m, p) for m, p in self.calls if m != "GET"]


def test_dry_run_writes_nothing(monkeypatch, capsys):
    """The defect this test exists for shipped and was found by review.

    Only the final POST /1/boards was behind the dry-run guard; the saved queries
    and query annotations each panel needs were created unconditionally. A dry
    run against a fresh environment issued ten writes and left five orphaned
    annotations, while printing only "would create board" -- and every repeat run
    created five more. A flag documented as "writing nothing" that writes is a
    false statement, not an imprecise one.
    """
    fake = FakeAPI()
    monkeypatch.setattr(honeycomb, "api", fake)
    honeycomb.apply("k", "Metrics", "ops@example.com", dry_run=True)

    assert fake.writes() == [], (
        f"--dry-run issued {len(fake.writes())} writes: {fake.writes()}"
    )


def test_a_real_run_does_write(monkeypatch, capsys):
    """The counterpart, so the test above cannot be satisfied by a tool that has
    simply stopped working."""
    fake = FakeAPI()
    monkeypatch.setattr(honeycomb, "api", fake)
    honeycomb.apply("k", "Metrics", "ops@example.com", dry_run=False)

    paths = [p for _, p in fake.writes()]
    assert any(p.startswith("/1/triggers") for p in paths)
    assert "/1/boards" in paths
    assert any(p.startswith("/1/queries") for p in paths)


def test_missing_run_queries_still_provisions(monkeypatch, capsys):
    """'Run Queries' is Enterprise-only. Requiring it meant the tool could not run
    at all on the tier most people provisioning this are on -- it exited 1 and
    left the account untouched. It now provisions and reports what it could not
    check."""
    fake = FakeAPI(access={"triggers": True, "boards": True,
                           "recipients": True, "queries": False})
    monkeypatch.setattr(honeycomb, "api", fake)
    rc = honeycomb.apply("k", "Metrics", "ops@example.com", dry_run=False)

    assert rc == 0, "a free-tier user provisioning successfully is a success"
    assert any(p.startswith("/1/triggers") for _, p in fake.writes()), "it must still provision"
    out = capsys.readouterr().out
    assert "NOT VERIFIED" in out
    assert "Enterprise" in out
    for spec in honeycomb.triggers():
        assert spec["name"] in out, "every unverified trigger must be named"
    assert not any(p.startswith("/1/query_results") for _, p in fake.calls), \
        "it must not attempt a query it has no permission to run"


def test_no_recipient_does_not_require_the_recipients_permission(monkeypatch):
    """--no-recipient never touches a recipient, so demanding the permission to
    manage one turned an unrelated missing checkbox into a hard stop."""
    fake = FakeAPI(access={"triggers": True, "boards": True,
                           "recipients": False, "queries": True})
    monkeypatch.setattr(honeycomb, "api", fake)
    assert honeycomb.apply("k", "Metrics", None, dry_run=False) == 0
    assert not any(p == "/1/recipients" for _, p in fake.calls)


def test_missing_a_genuinely_required_permission_still_stops(monkeypatch):
    """The relaxation must not become a blanket one: without Manage Triggers the
    tool cannot do its job at all, and should say so rather than half-run."""
    fake = FakeAPI(access={"triggers": False, "boards": True,
                           "recipients": True, "queries": True})
    monkeypatch.setattr(honeycomb, "api", fake)
    with pytest.raises(SystemExit) as e:
        honeycomb.apply("k", "Metrics", "ops@example.com", dry_run=False)
    assert "triggers" in str(e.value)


# The current API nests an email recipient's address under details.email_address.
# This read "target" or "address", matched nothing, and so wanted to create a
# recipient that already existed on every single run -- which is exactly what
# --dry-run is documented to catch, and did, once it was pointed at a
# provisioned account.
def test_recipient_target_reads_the_nested_address():
    assert honeycomb.recipient_target(
        {"id": "r1", "type": "email", "details": {"email_address": "ops@example.com"}}
    ) == "ops@example.com"


def test_recipient_target_still_reads_the_older_flat_shape():
    """Kept as a fallback rather than replaced, so a response from either shape
    matches instead of silently duplicating."""
    assert honeycomb.recipient_target({"type": "email", "target": "ops@example.com"}) == "ops@example.com"


def test_recipient_target_handles_other_types():
    """A Slack or PagerDuty target nests under the same key with a different
    field, and would otherwise reintroduce duplicate-on-every-run."""
    assert honeycomb.recipient_target(
        {"type": "slack", "details": {"slack_channel": "#alerts"}}
    ) == "#alerts"


def test_an_already_provisioned_account_creates_nothing_new(monkeypatch, capsys):
    """The tool's own idempotency claim, asserted against a full account.

    The module docstring says everything is matched by name and updated in place.
    That was true of the triggers, false of the recipient (wrong field), and false
    of the board (create-only, so a changed panel could never reach a provisioned
    account). This is the test that would have caught both.
    """
    fake = FakeAPI(
        triggers=[{"id": "t1", "name": honeycomb.PAGE_TRIGGER},
                  {"id": "t2", "name": honeycomb.NOTIFY_TRIGGER}],
        boards=[{"id": "b1", "name": honeycomb.BOARD_NAME}],
        recipients=[{"id": "r1", "type": "email",
                     "details": {"email_address": "ops@example.com"}}],
    )
    monkeypatch.setattr(honeycomb, "api", fake)
    honeycomb.apply("k", "Metrics", "ops@example.com", dry_run=False)

    posts = [p for m, p in fake.writes() if m == "POST"]
    assert "/1/recipients" not in posts, "created a recipient that already existed"
    assert "/1/boards" not in posts, "created a second board instead of updating the first"
    # And the board IS reconciled rather than skipped, which is the other half.
    assert ("PUT", "/1/boards/b1") in fake.writes(), (
        f"existing board was not updated; writes were {fake.writes()}"
    )


# The API rejects a list of bare ids with "422: incorrect type for field" and
# declines to say which field. A TriggerNotificationRecipient is an object whose
# id names an existing recipient, and nothing in the test suite checked the shape
# of what was actually being sent -- only that a recipient was or wasn't created.
def test_trigger_recipients_are_objects_not_ids(monkeypatch):
    sent = []

    def fake(key, method, path, body=None):
        if path == "/1/auth":
            return {"type": "configuration", "team": {"slug": "t"},
                    "environment": {"slug": "e"},
                    "api_key_access": {"triggers": True, "boards": True,
                                       "recipients": True, "queries": False}}
        if path == "/1/recipients":
            return [{"id": "r1", "type": "email",
                     "details": {"email_address": "ops@example.com"}}]
        if path.startswith("/1/triggers"):
            if method == "GET":
                return [{"id": "t1", "name": honeycomb.PAGE_TRIGGER},
                        {"id": "t2", "name": honeycomb.NOTIFY_TRIGGER}]
            sent.append(body)
            return {"id": "t1", "query_id": "q1"}
        if path == "/1/boards":
            return [{"id": "b1", "name": honeycomb.BOARD_NAME}] if method == "GET" else {
                "id": "b1", "links": {"board_url": "u"}}
        if path.startswith("/1/queries"):
            return {"id": "q1"}
        if path.startswith("/1/query_annotations"):
            return {"id": "a1"}
        return {}

    monkeypatch.setattr(honeycomb, "api", fake)
    honeycomb.apply("k", "Metrics", "ops@example.com", dry_run=False)

    assert sent, "no trigger was written"
    for body in sent:
        for r in body["recipients"]:
            assert isinstance(r, dict), f"recipient sent as {type(r).__name__}: {r!r}"
            assert r.get("id"), f"recipient object carries no id: {r!r}"
