#!/usr/bin/env python3
"""Drive real traffic through the local stack so the dashboard has a shape.

The savings dashboard groups spend by provider and model. With one route in the
policy every request lands in the same place, so the treemap is one box and shows
nothing about what routing is for.

Spreading it is not a matter of seeding rows. Policy routes are a failover order
rather than a menu -- every request takes route one unless it fails -- so the only
truthful way to move traffic between providers is to move the policy, which is
also how routing shifts in production. This publishes a sequence of signed
policies and drives a batch of real requests against each.

Every row it produces is a request that actually traversed the gateway, the
signed policy and a provider. Nothing is written into the telemetry table
directly: a dashboard whose claim is "every figure is read from this control
plane's own telemetry" cannot have its demonstration data inserted behind the
telemetry's back.

The last phase is the one routing cannot explain. The policy stays fixed and,
midway through, the mock provider starts answering openai as a model named
gpt-4o-mini-simulated-snapshot: the same route, the same provider, and a
different model saying it served the request, which is what a provider moving an
alias to a new snapshot looks like. It is labelled as simulated before it is
made -- an annotation on the control plane, and a Honeycomb marker when
HONEYCOMB_CONFIG_KEY is set in the calling shell -- so no chart can show it as a
change a provider really made.

Run it with scripts/dev-traffic.sh (make dev-traffic), which also mounts
controlplane/ and passes HONEYCOMB_CONFIG_KEY through. By hand, from inside the
task network namespace, the way scripts/dev-smoke.sh runs:

  docker run --rm -i --network container:switchboard-taskns-1 \\
    --env-file .dev/env -v "$PWD/scripts:/app/scripts:ro" \\
    -w /app --entrypoint python switchboard-dev:latest scripts/dev-traffic.py
"""

import json
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request

CONTROL = os.environ.get("CONTROL_URL", "http://127.0.0.1:8000")
GATEWAY = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8080")
ADMIN = os.environ["BOOTSTRAP_ADMIN_TOKEN"]
LOCAL = os.environ["LOCAL_TOKEN"]
TENANT = os.environ["TENANT"]
MOCK = os.environ.get("MOCK_URL", "http://127.0.0.1:9090")
HONEYCOMB_KEY = os.environ.get("HONEYCOMB_CONFIG_KEY", "").strip()

# Deterministic, so a rerun produces the same picture and a change in the
# dashboard is a change in the code rather than in the dice.
random.seed(17)

# The dev stack points openai and anthropic at the healthy mock and gemini at the
# one that answers nothing but 503. So the third phase is a real failover on
# every request rather than a simulated one.
PHASES = [
    ("openai first", [("openai", "gpt-4o-mini"), ("anthropic", "claude-sonnet-4")], 45),
    ("anthropic first", [("anthropic", "claude-sonnet-4"), ("openai", "gpt-4o-mini")], 30),
    ("gemini first, which is down", [("gemini", "gemini-2.0-flash"), ("openai", "gpt-4o-mini")], 25),
]

# The served-model phase. Its injected model has "simulated" in its name, so the
# word is in the data everywhere it appears rather than only in a label beside
# it. No random draws, and it runs after PHASES, so their picture is unchanged.
DRIFT = {
    "name": "openai first, policy fixed; the mock's openai snapshot changes midway",
    "routes": [("openai", "gpt-4o-mini"), ("anthropic", "claude-sonnet-4")],
    "before": 40, "after": 40, "settle": 8, "pace": 0.75,
    "flip_to": "gpt-4o-mini-simulated-snapshot",
}
DRIFT_PROMPT = "Reply with one short sentence about routing."

# One X-Switchboard-Route hop, read from the right as console.html reads it: the
# status is the last colon followed by a number or "skipped", because a model id
# can contain a colon.
HOP = re.compile(r"^(.*):(skipped|[0-9-]+)(?:\s+.*)?$")


def publish(routes, version):
    """Publish a signed policy, stepping the version until one is accepted.

    Version rollback and equivocation protection means a version already
    published cannot be replaced, so a rerun has to move forward rather than
    overwrite. Returning the accepted version lets the next phase continue from
    it instead of guessing.
    """
    now = int(time.time())
    while version < 10_000:
        body = json.dumps({
            "schema": 1, "tenant": TENANT, "version": version,
            "issued_at": now, "expires_at": now + 3600,
            "routes": [{"provider": p, "model": m} for p, m in routes],
        }).encode()
        req = urllib.request.Request(CONTROL + "/v1/policy", method="PUT", data=body, headers={
            "Authorization": "Bearer " + ADMIN, "Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=15).read()
            return version
        except urllib.error.HTTPError as e:
            if e.code != 409:
                raise SystemExit("publishing v%d failed: %d %s" % (version, e.code, e.read().decode()[:200]))
            version += 1
    raise SystemExit("could not find a free policy version")


def send(max_tokens, prompt):
    body = json.dumps({"model": "preferred", "max_tokens": max_tokens,
                       "messages": [{"role": "user", "content": prompt}]}).encode()
    req = urllib.request.Request(GATEWAY + "/v1/chat/completions", data=body, headers={
        "Authorization": "Bearer " + LOCAL, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, r.headers.get("X-Switchboard-Route", "")
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("X-Switchboard-Route", "")
    except Exception:
        return 0, ""


def wait_for(expected_first, timeout=90):
    """Wait until the gateway is actually routing to the new policy.

    The gateway polls the control plane on a 15-second ticker, so a fixed sleep
    is either too short and silently attributes a batch to the previous policy,
    or too long. This probes with a real request and reads the route it took,
    which is the condition that actually matters.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, route = send(64, "readiness probe")
        if route.startswith(expected_first + "/"):
            return True
        time.sleep(3)
    return False


def mark(message, kind):
    """A Honeycomb marker, when HONEYCOMB_CONFIG_KEY is set in the calling shell.

    Secondary to everything else here: the dashboard labels a simulated change
    from its annotation, not from a marker, so a marker that cannot be written is
    said and skipped rather than stopping the run.
    """
    if not HONEYCOMB_KEY:
        return
    try:
        from controlplane import honeycomb
        honeycomb.marker(HONEYCOMB_KEY, message, kind)
    except (ImportError, SystemExit) as e:
        print("  no Honeycomb marker: %s" % str(e).splitlines()[0])


def post_json(url, body, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, method="POST", data=json.dumps(body).encode(), headers=headers)
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read() or b"null")


def mock_control(dialect, served_model):
    """Tell the mock provider which model to say served this dialect's responses."""
    try:
        return post_json(MOCK + "/control", {"dialect": dialect, "served_model": served_model})
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise SystemExit("the mock provider has no /control endpoint. It needs MOCK_CONTROL=1, which "
                             "docker-compose.dev.yml sets; recreate the mockprovider container.")
        raise SystemExit("mock control failed: %d %s" % (e.code, e.read().decode()[:200]))


def annotate(route_model, served_model, version):
    """Record, before it happens, that the next served-model change is simulated.

    Before rather than after: if the label cannot be written the change is not
    made, so the dashboard never shows an injected change as an observed one.
    """
    try:
        return post_json(CONTROL + "/v1/annotations", {
            "kind": "simulated_served_model_change", "provider": "openai",
            "route_model": route_model, "served_model": served_model,
            "policy_version": version, "source": "mockprovider"}, ADMIN)
    except urllib.error.HTTPError as e:
        raise SystemExit("could not record the simulated change (HTTP %d). The control plane needs "
                         "SWITCHBOARD_DEV=1, which docker-compose.dev.yml sets. No change was made." % e.code)


def served_suffix(route_header):
    """What the answering hop says served it.

    "" when served as the model sent, "?" when the provider named none, the model
    otherwise, and None when there is no parseable hop at all.
    """
    if not route_header:
        return None
    m = HOP.match(route_header.split(",")[-1].strip())
    if not m:
        return None
    model = m.group(1).split("/", 1)[-1]
    return model.rsplit("=", 1)[1] if "=" in model else ""


def served_in_series(route_model):
    """(policy version, served model) pairs the control plane has answered requests under."""
    q = urllib.parse.urlencode([("hours", 1), ("by", "policy_version,served_model"),
                                ("measures", "answered"), ("where", "model:" + route_model)])
    req = urllib.request.Request(CONTROL + "/v1/series?" + q, headers={"Authorization": "Bearer " + ADMIN})
    with urllib.request.urlopen(req, timeout=15) as r:
        return {tuple(c["key"]) for c in json.loads(r.read())["cells"] if c["answered"]}


def drift(version):
    """Fixed policy; midway, the mock starts answering openai as a new snapshot.

    Nothing about routing changes -- same policy, same route, same provider -- so
    the one thing that moves is what the provider says served the request. That
    is the change a gateway cannot see without recording the served model.
    """
    name, routes, flip_to = DRIFT["name"], DRIFT["routes"], DRIFT["flip_to"]
    route_model = routes[0][1]
    version = publish(routes, version)
    print("policy v%d: %s" % (version, name))
    mark("policy v%d published: %s" % (version, name), "policy-publish")
    if not wait_for(routes[0][0]):
        print("  gave up waiting for the gateway to route to %s; skipping this phase" % routes[0][0])
        return version + 1

    def batch(n):
        out = []
        for _ in range(n):
            out.append(served_suffix(send(256, DRIFT_PROMPT)[1]))
            time.sleep(DRIFT["pace"])
        return out

    mock_control("openai", "echo")
    flipped = False
    try:
        before = batch(DRIFT["before"])
        annotate(route_model, flip_to, version)
        reply = mock_control("openai", flip_to)
        flipped = True
        mark("SIMULATED by mockprovider: openai now answers %s as %s; policy v%d unchanged"
             % (route_model, flip_to, version), "simulated")
        print("  %s: the mock now answers openai as %s (was %s)"
              % (time.strftime("%H:%M:%S", time.localtime(reply["effective_at"])), flip_to, reply["previous"]))
        after = batch(DRIFT["after"])
    finally:
        if flipped:
            # Labelled and then followed by traffic, so the change back is shown
            # as simulated too rather than surfacing later as an observed one.
            try:
                annotate(route_model, route_model, version)
                mark("SIMULATED by mockprovider: openai answers %s as itself again" % route_model, "simulated")
            except SystemExit as e:
                print("  could not label the restore: %s" % e)
        mock_control("openai", "echo")
    batch(DRIFT["settle"])

    as_sent = before.count("")
    switched = after.count(flip_to)
    print("  before: %d/%d served as sent; after: %d/%d served as %s" % (as_sent, len(before), switched, len(after), flip_to))
    # A request or two can straddle the switch; more than that is a real miss.
    if as_sent < len(before) - 2 or switched < len(after) - 2:
        raise SystemExit("self-check failed: the route header did not show the switch. Either the gateway "
                         "predates served-model telemetry or the mock did not change what it returns.")

    deadline = time.time() + 30
    while True:
        seen = served_in_series(route_model)
        versions = {v for v, served in seen if served == flip_to}
        if versions:
            break
        if time.time() > deadline:
            raise SystemExit("self-check failed: /v1/series never showed %s; is telemetry reaching the control plane?" % flip_to)
        time.sleep(2)
    if versions != {str(version)}:
        raise SystemExit("self-check failed: %s appears under policy versions %s, expected only v%d"
                         % (flip_to, sorted(versions), version))
    print("  /v1/series shows %s under v%d only: the model changed and the policy did not" % (flip_to, version))
    return version + 1


def main():
    version = 3
    for name, routes, count in PHASES:
        first = routes[0][0]
        version = publish(routes, version)
        print("policy v%d: %s" % (version, name))
        mark("policy v%d published: %s" % (version, name), "policy-publish")
        if not wait_for(first):
            print("  gave up waiting for the gateway to route to %s; skipping this phase" % first)
            version += 1
            continue

        landed = {}
        for _ in range(count):
            # Varied, so duration and token counts spread instead of landing on
            # one value and making every chart a single bar.
            status, route = send(
                random.choice([64, 128, 256, 512, 1024]),
                "x" * random.randint(40, 2500))
            hops = [h.strip().split(":")[0] for h in route.split(",")] if route else ["(none)"]
            landed[(hops[-1], status)] = landed.get((hops[-1], status), 0) + 1
        for (where, status), n in sorted(landed.items()):
            print("  %-34s %s  x%d" % (where, status, n))
        version += 1

    version = drift(version)

    print("\ndone. /dashboard now has more than one route in it, and /dashboard/drift shows a")
    print("served-model change labelled simulated.")


if __name__ == "__main__":
    main()
