"""What Switchboard thinks is worth waking someone for, with no backend in it.

Everything here is a fact about the gateway: which counters mean trouble, which
of them is an outage and which is a bill, and what a person needs to read at
three in the morning. Nothing here knows what a trigger, a rule or an alert
object looks like. The emitters beside this file do that -- ``honeycomb.py``
speaks to Honeycomb's API, ``prometheus.py`` writes alerting rules -- and a third
backend should be a third emitter rather than an edit to any of this.

**Metric names here carry no prefix, and that is the point.** The gateway
publishes the same counter under two spellings: the Prometheus exposition on
``/metrics`` emits ``switchboard_empty_completion_failed_total`` and the OTLP
exporter emits ``switchboard.empty_completion_failed_total``. Writing a trigger
against the wrong one produces a trigger referencing a column that does not
exist, which Honeycomb accepts and never fires -- and that is not hypothetical,
it is what happened. Holding the bare name here and letting each emitter apply
the spelling its own backend uses turns that from something to remember into
something that cannot be written.

Names must match ``series()`` in ``internal/gateway/telemetry.go``, which is the
single list both gateway export paths are built from.
"""
from typing import NamedTuple

# Urgency is a routing decision, not a severity score. "page" means a person is
# woken; "notify" means it waits for business hours. Anything between those two
# is a number someone has to interpret under stress, so there are only two.
PAGE = "page"
NOTIFY = "notify"


class Condition(NamedTuple):
    """One thing worth alerting on.

    ``window`` is how far back an evaluation looks. These are cumulative
    counters, so the question a backend has to answer is always "did this go up
    during the window", never "what is its total".
    """

    key: str
    metric: str
    urgency: str
    summary: str
    detail: str
    window: int


CONDITIONS = (
    Condition(
        key="lost_service",
        metric="empty_completion_failed_total",
        urgency=PAGE,
        summary="Lost service: every route returned nothing",
        detail=(
            "Every route produced no output and the caller received a 503. This is lost service, "
            "not degraded service. Recovered empty completions are deliberately not alerted; those "
            "are spend and latency, not an outage."
        ),
        # Shorter than the others on purpose. This is the only one that wakes
        # someone, so staleness costs more than an extra evaluation.
        window=300,
    ),
    Condition(
        key="account_failover",
        metric="account_failover_total",
        urgency=NOTIFY,
        summary="A provider account cannot serve",
        detail=(
            "A provider account is out of credits, below its balance floor, or has lost its quota. "
            "Usually a billing action rather than an incident, but it is silent capacity loss "
            "until someone pays it."
        ),
        window=900,
    ),
    Condition(
        key="usage_mismatch",
        metric="usage_mismatch_total",
        urgency=NOTIFY,
        summary="A provider's token totals did not add up",
        detail=(
            "A provider's own usage block was internally inconsistent, so metering taken from that "
            "response may be wrong in either direction and any invoice covering the window is "
            "suspect. Requests still succeeded; this is not an availability problem."
        ),
        window=900,
    ),
    Condition(
        key="idempotent_unknown",
        metric="idempotent_unknown_total",
        urgency=NOTIFY,
        summary="A retry was refused as ambiguous",
        detail=(
            "The original outcome could not be established, so the retry was refused rather than "
            "risk charging twice. Nobody was billed twice, which is the point, but repeated "
            "ambiguity is worth understanding."
        ),
        window=900,
    ),
)

# Counters that look alertable and deliberately are not, recorded here so the
# decision survives someone reading the list and finding it short.
NOT_ALERTED = {
    "empty_completion_recovered_total": (
        "A later route answered, so the caller was served. It is spend and latency rather than an "
        "outage, and budget-aware routing should drive it toward zero on its own. Graph it."
    ),
    "empty_completion_total": (
        "Counted per route, so one request can advance it more than once. Useful as a rate against "
        "requests_total, misleading as a threshold."
    ),
    "errors_total": (
        "Includes every 4xx. A caller sending malformed requests is not an incident, and alerting "
        "on this trains people to ignore it."
    ),
    "retries_total": (
        "Retries working is the system working. This matters as a ratio against requests_total, "
        "which is a graph rather than a threshold."
    ),
}


class Panel(NamedTuple):
    """A group of metrics that answer one question when read together."""

    title: str
    metrics: tuple[str, ...]
    caption: str


PANELS = (
    Panel(
        title="Which one moved",
        metrics=("account_failover_total", "usage_mismatch_total", "idempotent_unknown_total"),
        caption=(
            "The three notify conditions, separately. A backend that cannot give each its own "
            "alert sends you here to find out which one fired."
        ),
    ),
    Panel(
        title="Empty completions",
        metrics=(
            "empty_completion_total",
            "empty_completion_recovered_total",
            "empty_completion_failed_total",
        ),
        caption=(
            "Recovered means a later route answered: the caller was served, but two providers were "
            "paid and both were waited for. Failed means nobody answered."
        ),
    ),
    Panel(
        title="Idempotency",
        metrics=(
            "idempotent_replay_total",
            "idempotent_conflict_total",
            "idempotent_unknown_total",
        ),
        caption=(
            "Replay is the mechanism working. Conflict is a reused key with a different body. "
            "Unknown is the sticky state, where the original outcome could not be established."
        ),
    ),
    Panel(
        title="Traffic, errors, retries",
        metrics=(
            "requests_total",
            "errors_total",
            "retries_total",
            "rate_limited_total",
        ),
        caption=(
            "Denominator for everything above. Retries and rate limiting without a matching rise "
            "in errors means the gateway absorbed provider trouble rather than passing it on."
        ),
    ),
)

class SpanPanel(NamedTuple):
    """A question answered from the gateway's request spans rather than its counters.

    ``group_by`` and the attribute inside ``measure`` are full span attribute
    names, not suffixes: they follow the GenAI conventions where one applies and
    the gateway's own namespace where none does, and no emitter prefixes them.
    ``measure`` is ``count`` or ``distinct:<attribute>``.
    """

    title: str
    group_by: tuple[str, ...]
    measure: str
    caption: str


SENT_MODEL = "gen_ai.request.model"
SERVED_MODEL = "gen_ai.response.model"

# Served-model drift: a provider answering a request for one model with another.
#
# Deliberately not alerted. The two trigger slots the Honeycomb free plan allows
# are taken by the page and the combined notify, and neither should give way;
# and providers move aliases to new snapshots as a matter of course, so "the
# served model changed" is something to see on a graph and look into, not
# something to wake anyone for. Written here rather than in NOT_ALERTED, whose
# keys are counter names.
SPAN_PANELS = (
    SpanPanel(
        title="Served model under each sent model",
        group_by=(SENT_MODEL, SERVED_MODEL),
        measure="count",
        caption=(
            "Answered requests, split by the model sent and the model the provider said served "
            "it. A new served value under an unchanged sent model is a provider moving an alias. "
            "A blank served value means the provider did not say, which Bedrock never does."
        ),
    ),
    SpanPanel(
        title="Distinct served models per sent model",
        group_by=(SENT_MODEL,),
        measure="distinct:" + SERVED_MODEL,
        caption=(
            "How many different models answered for each model sent. A step from one to two is "
            "the moment a snapshot changed. Blanks are not counted, so a provider that stops "
            "naming its model shows only in the panel above."
        ),
    ),
    SpanPanel(
        title="Answers by served model, per policy version",
        group_by=("switchboard.policy_version", SERVED_MODEL),
        measure="count",
        caption=(
            "The mix of models that answered under each signed policy. A shift inside one version "
            "is the providers or the circuit breaker; a shift at a version boundary is the policy."
        ),
    ),
)

# The latency histogram is named separately because it is not a counter and the
# emitters aggregate it differently. Its percentiles are not currently
# trustworthy: traffic below the lowest bucket bound lands in a bucket with no
# lower edge, so a percentile there extrapolates below zero. See docs/GAPS.md.
#
# Unlike every counter here this is a FULL column name, not a suffix the emitter
# prefixes with "switchboard.". It follows the OpenTelemetry GenAI conventions,
# which own this measurement and name it in seconds; the counters describe
# routing, which no convention covers, and keep their own namespace. Anything
# built on the old switchboard.request_duration_milliseconds column stops
# resolving and has to be recreated.
LATENCY_METRIC = "gen_ai.server.request.duration"

OVERVIEW = """Two things to know before reading an alert from this service.

`empty_completion_failed_total` is the only condition that is an outage: every route produced no
output and the caller got a 503. Everything else is money or ambiguity, and waits.

`empty_completion_recovered_total` deliberately has no alert. A later route answered, so the caller
was served; it is spend and latency, and budget-aware routing should push it toward zero on its own.
Watch it on a graph instead."""


def by_urgency(urgency: str) -> tuple[Condition, ...]:
    return tuple(c for c in CONDITIONS if c.urgency == urgency)


def condition(key: str) -> Condition:
    for c in CONDITIONS:
        if c.key == key:
            return c
    raise KeyError(f"no condition named {key!r}")
