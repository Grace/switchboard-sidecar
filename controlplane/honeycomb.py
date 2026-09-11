"""Provision the Honeycomb alerting for a Switchboard deployment.

The conditions come from ``alerting.py``, which knows nothing about Honeycomb.
This file is the Honeycomb emitter: it applies the OTLP spelling, folds the
conditions to fit this vendor's trigger cap, and talks to the API.

The alerting for this gateway existed for a while only because someone made a
series of API calls by hand, which meant a deployer got the documented advice to
create triggers and no means of doing it. This creates the same two triggers, one
recipient and one board in any Honeycomb environment, from flags.

    python -m controlplane.honeycomb --dataset Metrics --recipient ops@example.com

There is no --env flag. A v1 API key is scoped to one environment, so the key
already chooses it and a flag would only imply a choice that is not there. The
key is read from HONEYCOMB_CONFIG_KEY rather than an argument so it stays out of
the process table and shell history, as the signing seed does in policytool. Not
HONEYCOMB_API_KEY: that holds the ingest key a gateway itself reads, and this
tool refuses to fall back to it -- see main().

Three behaviours here are not incidental, and each one is a bruise:

Re-running is safe. Everything is matched by name and updated in place. That is
not tidiness: the Honeycomb free plan allows two triggers per team, so a second
blind create *cannot* succeed, and a provisioning tool that only works against an
empty account is a tool you cannot run twice. The board was the exception to this
for longer than it should have been -- it was created once and thereafter skipped,
so a changed panel never reached a provisioned account.

Failures print the API's own words. Tooling in front of this API reported
"Failed to save trigger" for a plan-limit rejection and for an invalid
aggregation alike, which cost an hour of looking at the query. Calling the API
directly returned the real message immediately in both cases.

Every trigger's query is executed before this exits. Honeycomb will accept a
trigger whose query it refuses to run, show it as healthy in the trigger list,
and never fire it. Both triggers in the first account this ran against were in
exactly that state for hours. Creating a trigger is therefore not evidence that
the trigger works, and the only evidence that is, is running its query.
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

from . import alerting

API = "https://api.honeycomb.io"

BOARD_NAME = "Switchboard gateway"

# The page trigger's name comes from the condition itself. The notify trigger is
# an invention of this emitter -- it corresponds to no single condition, only to
# the plan limit that forced three of them together -- so it is named here.
PAGE_TRIGGER = alerting.condition("lost_service").summary
NOTIFY_TRIGGER = "Something needs a person: billing, metering or an ambiguous retry"


class Fatal(SystemExit):
    def __init__(self, msg):
        super().__init__(f"honeycomb: {msg}")


def api(key: str, method: str, path: str, body=None):
    """One HTTP call, with the server's own error text preserved.

    urllib raises HTTPError before a caller can read the body, and the body is
    the only part worth having: "exceeded maximum 2 triggers for this team's
    plan" and "aggregate operation not allowed in Metrics dataset: RATE_SUM" are
    both actionable, and both arrive as a bare 400 or 422 otherwise.
    """
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method)
    req.add_header("X-Honeycomb-Team", key)
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace").strip()
        try:
            parsed = json.loads(detail)
            detail = parsed.get("error", detail)
            # type_detail is where this API puts the part worth having. The
            # top-level error reads "The provided input is invalid." for a
            # missing field, a wrong type, and a field that is merely not allowed
            # on writes alike; only this array says which field and why. Leaving
            # it unread cost an hour on "dataset is not allowed on query panels
            # requests", which the server had been saying all along.
            for d in parsed.get("type_detail") or []:
                detail += "\n    {}: {}".format(
                    d.get("field", "?"), d.get("description") or d.get("code", ""))
        except Exception:
            pass
        if "maximum" in detail and "plan" in detail:
            raise Fatal(
                f"{detail}\n"
                "  This is a plan limit, not a problem with the query. The free plan allows two\n"
                "  triggers per team; docs/DEPLOYMENT.md shows how four conditions fit into two."
            )
        raise Fatal(f"{method} {path} -> {e.code}: {detail}")
    except urllib.error.URLError as e:
        raise Fatal(f"{method} {path} -> {e.reason}")


# What each Honeycomb permission is needed for. Named individually because
# "isn't allowed" tells a deployer nothing about which switch to flip, and the
# first key handed to this tool had two of the four off.
# Permissions this tool cannot work without.
REQUIRED = {
    "triggers": "create and update the two triggers",
    "boards": "create the board",
}
# Required only when a recipient is actually being set. --no-recipient touches none.
CONDITIONAL = {
    "recipients": "attach a notification target",
}
# Wanted, but NOT required, and the distinction matters more than it looks.
#
# "Run Queries" is an Enterprise-plan permission. Making it mandatory - which an
# earlier version of this did - means the tool cannot run at all on a free or
# self-serve plan, which is the tier most people provisioning this are on. A
# provisioning tool that refuses to provision is worse than one that provisions
# and says clearly what it could not check.
#
# So without it: do the work, then say loudly which triggers went out unverified
# and how to check them by hand. With it: prove every query runs, because a
# trigger Honeycomb stores and refuses to execute looks identical to a healthy
# one in the UI, and that is not a hypothetical - it is what two triggers in the
# first account this ran against were doing for hours.
OPTIONAL = {
    "queries": "execute each trigger's query to prove it runs (Enterprise plans only)",
}
NEEDED = {**REQUIRED, **CONDITIONAL, **OPTIONAL}


def preflight(key: str, want_recipient: bool = True) -> dict:
    """Ask the key what it can do, before using it for anything.

    An earlier version of this guessed from a failed write that the key was an
    ingest key. It was not; it was a configuration key with two permissions
    switched off, and /1/auth would have said so in one request. Guessing about
    a fact the server will state is how an afternoon goes missing.

    Reporting the environment matters as much as the permissions. The key alone
    decides which environment is written to, so a key from the wrong one
    provisions a perfectly correct set of triggers somewhere nobody is looking.
    """
    auth = api(key, "GET", "/1/auth")
    env = (auth.get("environment") or {}).get("slug", "?")
    team = (auth.get("team") or {}).get("slug", "?")
    print(f"key: type={auth.get('type', '?')} team={team} environment={env}")

    access = auth.get("api_key_access") or {}
    need = dict(REQUIRED)
    if want_recipient:
        need.update(CONDITIONAL)

    missing = [p for p in need if not access.get(p)]
    if missing:
        lines = "\n".join(f"    {p:<12} to {need[p]}" for p in missing)
        raise Fatal(
            "this key is missing permissions it needs:\n" + lines + "\n"
            "  Enable them in Honeycomb under Environments > Manage Environments > your\n"
            "  environment > API Keys > Configuration Keys > Details.\n"
            "  An ingest key has none of these; it can send telemetry and nothing else.\n"
            "  Pass --no-recipient if you deliberately want triggers that notify nobody."
        )
    return auth


# Honeycomb receives the OTLP spelling: "switchboard." + name. The Prometheus
# exposition uses "switchboard_" + name instead, and alerting.py stores neither,
# so this prefix is the one place the dotted form is written.
PREFIX = "switchboard."


def column(metric: str) -> str:
    return PREFIX + metric


def counter(metric: str, alias: str, window: int) -> dict:
    """A query for one cumulative counter over the trigger's own window.

    SUM, never RATE_SUM. On a Metrics dataset Honeycomb applies the counter's
    temporal aggregation before this one, so SUM over the window is the increase
    during it rather than the running total, and RATE_SUM is refused outright by
    the query engine. A trigger holding one is accepted and then never evaluates.
    """
    return {
        "calculations": [{"column": column(metric), "op": "SUM", "name": alias}],
        "time_range": window,
    }


def combined(conditions, window: int) -> dict:
    """Several counters reduced to one value, so one trigger can watch them all.

    A trigger query may hold only one aggregate -- a second is refused with
    "only one non-having aggregate is allowed" -- but a formula collapses many
    into one, and formulas are permitted on a Metrics dataset.
    """
    return {
        "calculations": [
            {"column": column(c.metric), "op": "SUM", "name": c.key} for c in conditions
        ],
        "formulas": [
            {"name": "needs_a_human", "expression": " + ".join("$" + c.key for c in conditions)}
        ],
        "time_range": window,
    }


def triggers() -> list[dict]:
    """The conditions from alerting.py, folded to fit Honeycomb's trigger cap.

    Switchboard defines four conditions. The Honeycomb free plan allows two
    triggers per team, so the three notify conditions are summed into one and the
    page keeps a trigger to itself. That folding is a compromise with this
    vendor's pricing, not something Switchboard believes about its counters,
    which is why it happens here and not in alerting.py -- the Prometheus emitter
    reads the same definitions and writes four separate rules.

    The page stays alone deliberately. It is the only one of the four that is an
    outage, and merging anything into it would blunt the single alert that should
    wake someone.
    """
    out = []
    for cond in alerting.by_urgency(alerting.PAGE):
        out.append({
            "name": cond.summary,
            "description": f"{PREFIX}{cond.metric} increased. {cond.detail}",
            "query": counter(cond.metric, cond.key, cond.window),
            "threshold": {"op": ">", "value": 0},
            "frequency": cond.window,
            "alert_type": "on_change",
            "tags": [
                {"key": "service", "value": "switchboard"},
                {"key": "urgency", "value": cond.urgency},
            ],
        })

    notify = alerting.by_urgency(alerting.NOTIFY)
    if notify:
        # The notification carries this text and nothing else, and it cannot say
        # which of the three moved. So it names all three, and points at the
        # board, or it is an alert nobody can act on without guessing.
        detail = " ".join(f"{PREFIX}{c.metric}: {c.detail}" for c in notify)
        window = max(c.window for c in notify)
        out.append({
            "name": NOTIFY_TRIGGER,
            "description": (
                "One of several counters moved. They are watched together because the Honeycomb "
                "free plan allows two triggers in total, not because they belong together; open "
                f"the {BOARD_NAME} board to see which one moved. " + detail
            ),
            "query": combined(notify, window),
            "threshold": {"op": ">", "value": 0},
            "frequency": window,
            "alert_type": "on_change",
            "tags": [
                {"key": "service", "value": "switchboard"},
                {"key": "urgency", "value": alerting.NOTIFY},
            ],
        })
    return out


def sums(*metrics: str) -> dict:
    return {
        "calculations": [{"column": column(m), "op": "SUM"} for m in metrics],
        "time_range": 86400,
    }


def board_queries() -> list[tuple[str, str, dict]]:
    """The panels from alerting.py, plus the latency distribution.

    Latency is appended here rather than defined as a panel because it is a
    histogram and every backend aggregates one differently; HEATMAP is
    Honeycomb's answer and means nothing to Prometheus.
    """
    out = [(p.title, p.caption, sums(*p.metrics)) for p in alerting.PANELS]
    out.append((
        "Request duration by provider",
        "Seconds, not milliseconds: this follows the GenAI conventions, which specify seconds. "
        "Both known causes of the negative percentiles this panel used to show are fixed -- the "
        "bucket floor moved from 100ms to 5ms, and requests that never reached a provider are no "
        "longer in this population at all -- but whether the percentiles now read correctly has "
        "not been confirmed against real traffic, so treat the shape as the reliable part. "
        "A request that failed over is attributed to the provider that answered while its "
        "duration includes the ones that did not, which is what the caller waited and is not a "
        "per-provider service time.",
        {
            # Not through column(): LATENCY_METRIC is already a full column
            # name from the GenAI conventions, and prefixing it would produce
            # switchboard.gen_ai.server.request.duration, which nothing emits.
            "calculations": [{"column": alerting.LATENCY_METRIC, "op": "HEATMAP"}],
            # The reason the metric was dimensioned. Before this the histogram
            # was one global number and no per-provider comparison could be
            # drawn at all.
            "breakdowns": ["gen_ai.provider.name"],
            "time_range": 86400,
        },
    ))
    return out


BOARD_TEXT = alerting.OVERVIEW + """

The triggers on this environment cover those conditions in two slots, because the Honeycomb free
plan allows two triggers in total. The page names its own cause; the notify trigger sums several
counters and cannot say which moved, so the first panel below is the answer."""


def existing_columns(key: str, dataset: str) -> set[str]:
    """Every column name the dataset has actually received data for.

    Honeycomb refuses to store a query naming a column it has never seen --
    ``422: The provided input is invalid``, with no indication of which column.
    Measured, not assumed: a HEATMAP over an existing column is accepted and the
    same HEATMAP over an absent one is refused.

    That makes column existence a precondition of provisioning a board, and it
    bites hardest on exactly the case this tool exists for: a customer's
    environment is empty until their gateway sends something, so on a fresh
    environment every panel would be refused and the whole run would abort.
    """
    listed = api(key, "GET", f"/1/columns/{dataset}") or []
    return {c.get("key_name") for c in listed if c.get("key_name")}


def columns_used(spec: dict) -> set[str]:
    """The columns a query spec names, across calculations and breakdowns."""
    out = {c["column"] for c in spec.get("calculations", []) if c.get("column")}
    out |= set(spec.get("breakdowns") or [])
    return out


def notify(recipient_id: str) -> dict:
    """One entry in a trigger's ``recipients`` list.

    An object, not the bare id. The API rejects a list of strings with
    ``422: incorrect type for field`` and does not say which field, which is a
    long way from the two minutes it takes to read the schema: a
    TriggerNotificationRecipient is an object whose ``id`` names an existing
    recipient.

    A function rather than an inline dict at both call sites, because the two
    sites are the found-it and created-it branches of the same decision and a fix
    applied to one of them would look complete.
    """
    return {"id": recipient_id}


def recipient_target(r: dict) -> str | None:
    """Where a recipient's address actually lives in the API response.

    This read `target` or `address` and matched nothing, so every run wanted to
    create a recipient that already existed -- the tool was not idempotent, which
    is precisely what --dry-run exists to catch and duly did. The current API
    nests it: an email recipient carries `details.email_address`, and the flat
    fields are from an older shape.

    Kept as a fallback chain rather than one lookup because the other recipient
    types nest differently under the same key, and a Slack or PagerDuty target
    would otherwise reintroduce the same duplicate-on-every-run behaviour the
    moment one is used.
    """
    d = r.get("details") or {}
    return (
        d.get("email_address")
        or d.get("slack_channel")
        or d.get("pagerduty_integration_name")
        or d.get("webhook_name")
        or r.get("target")
        or r.get("address")
    )


def find_by_name(items, name: str):
    """Exact-name match, or None. The whole idempotency story rests on this.

    Exact rather than fuzzy on purpose: a near-match that silently updated the
    wrong trigger would be worse than creating a duplicate, and on a capped plan
    a duplicate is refused loudly anyway.
    """
    for it in items or []:
        if it.get("name") == name:
            return it
    return None


def run_query(key: str, dataset: str, query_id: str, name: str):
    """Execute a trigger's query and fail if the engine will not run it.

    A trigger is not proven by having been accepted. Honeycomb stores one whose
    query it refuses, reports it as healthy, and never fires it.
    """
    started = api(key, "POST", f"/1/query_results/{dataset}", {"query_id": query_id})
    rid = started.get("id")
    for _ in range(20):
        res = api(key, "GET", f"/1/query_results/{dataset}/{rid}")
        if res.get("complete"):
            return
        time.sleep(0.5)
    raise Fatal(f"query for {name!r} did not complete; treat the trigger as unverified")


def apply(key: str, dataset: str, recipient: str | None, dry_run: bool = False) -> int:
    """Reconcile the account against the definitions above.

    --dry-run exists because the honest way to check a provisioning tool is to
    point it at an account that is already provisioned and confirm it wants to
    change nothing. Doing that for real would mean writing to a live account to
    prove that writing was unnecessary.
    """
    auth = preflight(key, want_recipient=recipient is not None)
    can_verify = bool((auth.get("api_key_access") or {}).get("queries"))
    changed = []
    plan = []
    unverified = []

    def write(kind, name, fn):
        if dry_run:
            plan.append(f"would {kind}: {name}")
            return None
        return fn()

    recipients = []
    if recipient:
        listed = api(key, "GET", "/1/recipients") or []
        existing = find_by_name(
            [{**r, "name": recipient_target(r)} for r in listed], recipient
        )
        if existing:
            print(f"recipient exists: {recipient}")
            recipients = [notify(existing["id"])]
        else:
            made = write("create recipient", recipient,
                         lambda: api(key, "POST", "/1/recipients",
                                     {"type": "email", "target": recipient}))
            if made:
                print(f"recipient created: {recipient}")
                recipients = [notify(made["id"])]
                changed.append("recipient")

    have = api(key, "GET", f"/1/triggers/{dataset}") or []
    for spec in triggers():
        body = dict(spec, recipients=recipients)
        found = find_by_name(have, spec["name"])
        if found:
            got = write("update trigger", spec["name"],
                        lambda: api(key, "PUT", f"/1/triggers/{dataset}/{found['id']}", body))
            if got:
                print(f"trigger updated: {spec['name']}")
        else:
            got = write("create trigger", spec["name"],
                        lambda: api(key, "POST", f"/1/triggers/{dataset}", body))
            if got:
                print(f"trigger created: {spec['name']}")
                changed.append(spec["name"])
        # Verified even on a dry run, against the trigger already in place. The
        # point of the check is to catch a stored trigger whose query the engine
        # will not run, and that is exactly what a dry run should surface.
        qid = got["query_id"] if got else (found or {}).get("query_id")
        if qid and can_verify:
            run_query(key, dataset, qid, spec["name"])
            print(f"  query runs: {spec['name']}")
        elif qid:
            unverified.append(spec["name"])

    boards = api(key, "GET", "/1/boards") or []
    existing_board = find_by_name(boards, BOARD_NAME)
    # Reconciled rather than skipped. This used to print "board exists" and stop,
    # which meant a panel or caption changed in this file could never reach an
    # account that had already been provisioned -- and the module docstring above
    # claimed everything was "matched by name and updated in place". It was true
    # of the triggers and the recipient and false of the board, so the one object
    # a deployer would notice going stale was the one that never updated. The
    # live board spent two days explaining a defect that had been fixed.
    #
    # The cost, named because Honeycomb does not collect it: a saved query is
    # immutable, so an update mints new queries and annotations and the previous
    # ones are left in the account unreferenced. Five of each per update. That is
    # worth less than a board that silently never changes, but it is not free.
    panels = [
        {
            "type": "text",
            "position": {"x_coordinate": 0, "y_coordinate": 0, "width": 12, "height": 5},
            "text_panel": {"content": BOARD_TEXT},
        }
    ]
    have = existing_columns(key, dataset)
    panels_wanted = board_queries()
    skipped = []
    if have:
        keep = []
        for entry in panels_wanted:
            missing = sorted(columns_used(entry[2]) - have)
            if missing:
                skipped.append((entry[0], missing))
            else:
                keep.append(entry)
        panels_wanted = keep

    for i, (name, desc, spec) in enumerate(panels_wanted):
        # Behind write(), not around it. A board panel needs a saved query and
        # an annotation to point at, and creating those is as much a write as
        # creating the board: they are named objects that persist in the
        # account whether or not a board ever references them. Issuing them
        # during a dry run left five orphaned annotations per run and printed
        # nothing about it, which makes a flag documented as "writing nothing"
        # a false statement rather than an imprecise one.
        q = write("create query", name, lambda spec=spec:
                  api(key, "POST", f"/1/queries/{dataset}", spec))
        ann = write("create query annotation", name, lambda name=name, desc=desc, q=q:
                    api(key, "POST", f"/1/query_annotations/{dataset}",
                        {"name": name, "description": desc, "query_id": q["id"]}))
        if q is None or ann is None:
            # Dry run: there is no id to build a panel around, and inventing a
            # placeholder would produce a "would create" report describing a
            # board that could not be built from it.
            continue
        panels.append(
            {
                "type": "query",
                "position": {
                    "x_coordinate": 0 if i % 2 == 0 else 6,
                    "y_coordinate": 5 + (i // 2) * 4,
                    "width": 6,
                    "height": 4,
                },
                # No "dataset" key. The API returns it on a GET and refuses it
                # on a write -- "dataset is not allowed on query panels requests"
                # -- so a board read back and written straight out again is
                # rejected. The saved query already carries the dataset it was
                # created against.
                "query_panel": {
                    "query_id": q["id"],
                    "query_annotation_id": ann["id"],
                    "query_style": "combo",
                },
            }
        )
    body = {
        "name": BOARD_NAME,
        "description": "What the two triggers cannot tell you: which counter moved, "
        "and what the gateway was doing when it did.",
        "type": "flexible",
        "panels": panels,
        "tags": [{"key": "service", "value": "switchboard"}],
    }
    verb, method, path = (
        ("update board", "PUT", f"/1/boards/{existing_board['id']}")
        if existing_board
        else ("create board", "POST", "/1/boards")
    )
    made = write(verb, BOARD_NAME, lambda: api(key, method, path, body))
    if made:
        print(f"board {verb.split()[0]}d: {made['links']['board_url']}")
        changed.append(BOARD_NAME)

    if skipped:
        print()
        print("Panels left off the board, because their columns do not exist yet:")
        for name, missing in skipped:
            print(f"  - {name}: {', '.join(missing)}")
        print("  A column appears the first time data is written to it, so these")
        print("  become available once the gateway has exported once. Re-run then;")
        print("  the board is updated in place and will pick them up.")

    if unverified:
        # Loud, itemised, and last, so it is the part still on screen. Saying
        # "some checks were skipped" would be technically true and useless; the
        # operator needs to know which triggers are unproven and what to do.
        print()
        print("=" * 72)
        print("NOT VERIFIED. These triggers were written but not proven to run:")
        for name in unverified:
            print(f"  - {name}")
        print()
        print("  This key cannot execute queries. 'Run Queries' is an Enterprise-plan")
        print("  permission, so on a free or self-serve plan this check is unavailable and")
        print("  the tool does not pretend otherwise.")
        print()
        print("  It matters because Honeycomb will store a trigger whose query it refuses")
        print("  to run, list it as healthy, and never fire it. To check by hand: open each")
        print("  trigger, run its query in the query builder, and confirm it returns a")
        print("  number rather than an error.")
        print("=" * 72)

    print()
    if dry_run:
        print("\n".join(plan) if plan else "no changes; everything already exists")
    else:
        print("created: " + (", ".join(changed) if changed else
                             "nothing; everything already existed"))
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m controlplane.honeycomb",
        description="Create or update the Switchboard triggers, recipient and board in Honeycomb.",
    )
    ap.add_argument("--dataset", default="Metrics",
                    help="dataset holding the switchboard.* counters (default: Metrics)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--recipient", help="email address the triggers notify")
    g.add_argument("--no-recipient", action="store_true",
                   help="create the triggers notifying nobody; they will be visible in the "
                        "Honeycomb UI and will page no one, which is worse than having no "
                        "trigger because it reads as coverage. Required explicitly for that "
                        "reason rather than being the default.")
    ap.add_argument("--dry-run", action="store_true",
                    help="read the account and report what would change, writing nothing. "
                         "Trigger queries are still executed, since a stored trigger the engine "
                         "refuses to run is exactly what a dry run should catch.")
    a = ap.parse_args(argv)

    # Deliberately not HONEYCOMB_API_KEY. That variable holds the *ingest* key,
    # and a gateway reads it at request time to authenticate its OTLP export --
    # in the dev stack .dev/env is handed to the gateway container wholesale, so
    # anything in it is readable by the data plane. A configuration key placed
    # there would let the gateway rewrite or delete the alerting that watches it,
    # which is the same mistake as letting it sign the policy it enforces.
    #
    # There is no fallback to HONEYCOMB_API_KEY on purpose. A fallback would make
    # putting the configuration key in the wrong variable work, which is exactly
    # how it would end up there, and the resulting exposure is silent.
    key = os.environ.get("HONEYCOMB_CONFIG_KEY", "").strip()
    if not key:
        raise Fatal(
            "set HONEYCOMB_CONFIG_KEY to a Honeycomb Configuration key.\n"
            "  Not HONEYCOMB_API_KEY: that one holds the ingest key the gateway itself reads,\n"
            "  and a configuration key there would let the data plane edit its own alerting."
        )
    return apply(key, a.dataset, None if a.no_recipient else a.recipient, a.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
