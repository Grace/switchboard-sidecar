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

Run it from inside the task network namespace, the way scripts/dev-smoke.sh runs:

  docker run --rm -i --network container:switchboard-taskns-1 \\
    --env-file .dev/env -v "$PWD/scripts:/app/scripts:ro" \\
    -w /app --entrypoint python switchboard-dev:latest scripts/dev-traffic.py
"""

import json
import os
import random
import time
import urllib.error
import urllib.request

CONTROL = os.environ.get("CONTROL_URL", "http://127.0.0.1:8000")
GATEWAY = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8080")
ADMIN = os.environ["BOOTSTRAP_ADMIN_TOKEN"]
LOCAL = os.environ["LOCAL_TOKEN"]
TENANT = os.environ["TENANT"]

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


def main():
    version = 3
    for name, routes, count in PHASES:
        first = routes[0][0]
        version = publish(routes, version)
        print("policy v%d: %s" % (version, name))
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

    print("\ndone. the dashboard's window now has more than one route in it.")


if __name__ == "__main__":
    main()
