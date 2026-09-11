#!/usr/local/bin/python3
"""Local development bootstrap.

Collapses the manual first-run sequence — keypair, database role, tenant,
principals, signed policy, gateway config — into three commands the compose
stack calls in order. It is for local use only: it prints tokens to stdout and
writes them to disk, which is exactly what a deployment must never do.

  keys     generate Ed25519 signing material and the bearer tokens, as shell env
  dbinit   apply migrations and create the least-privileged runtime login
  init     provision tenant and principals, publish a signed policy, write config

Reuses controlplane.migrate and scripts.bootstrap rather than reimplementing
migration or tenant provisioning.
"""
import argparse
import base64
import hashlib
import json
import os
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def _b64(raw):
    return base64.b64encode(raw).decode()


# The marker dev-up.sh sets when it calls `keys`. Anything else has to opt in
# deliberately, in an environment variable, which is not something a person
# reaches for by accident and not something a stray `run-task` override carries.
DEV_MARKER = "SWITCHBOARD_DEV"


def cmd_keys(args):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives import serialization

    # This subcommand prints an Ed25519 signing seed and four bearer tokens to
    # stdout, which is correct for its purpose -- dev-up.sh redirects it into
    # .dev/env -- and indefensible anywhere else.
    #
    # It needs a guard because this file ships in the production image.
    # Dockerfile.controlplane copies scripts/, and quickstart.yaml's bootstrap
    # task runs `scripts/devstack.py dbinit`, so `devstack.py` cannot simply be
    # excluded: dbinit is the production entrypoint for migrations and the
    # runtime login. That leaves `keys` present and one run-task override away
    # from printing a signing seed into a CloudWatch log group.
    #
    # Nothing does that today. The point is that nothing should be able to.
    if not os.environ.get(DEV_MARKER):
        raise SystemExit(
            "refusing to run `keys` without %s=1 set.\n"
            "  It prints a signing seed and four bearer tokens to stdout, and this\n"
            "  file ships inside the production control-plane image, where stdout is\n"
            "  a CloudWatch log group. `make dev-up` sets the marker for you.\n"
            "  In a deployment, secrets come from Secrets Manager by ECS injection;\n"
            "  nothing should be generating them here." % DEV_MARKER)

    private = Ed25519PrivateKey.generate()
    seed = private.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public = private.public_key().public_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
    )
    # 43 base64 chars from 32 bytes, comfortably over the 32-byte minimum the
    # gateway and control plane both enforce on bearer tokens.
    out = {
        "POLICY_KEY_ID": args.key_id,
        "POLICY_SIGNING_SEED": _b64(seed),
        "POLICY_PUBLIC_KEY": _b64(public),
        "BOOTSTRAP_ADMIN_TOKEN": secrets.token_urlsafe(32),
        "CONTROL_TOKEN": secrets.token_urlsafe(32),
        "LOCAL_TOKEN": secrets.token_urlsafe(32),
        "APP_DB_PASSWORD": secrets.token_urlsafe(24),
        "TENANT": args.tenant,
    }
    for k, v in out.items():
        print("%s=%s" % (k, v))


# Every one of these comes from .dev/env, which `make dev-up` writes. A bare
# KeyError traceback names the variable and nothing else -- not where it should
# have come from, and not that running `docker compose up` directly instead of
# `make dev-up` is the usual reason it is missing, because compose interpolates
# from the shell rather than from env_file.
_ENV_SOURCE = {
    "DBHOST": "set by docker-compose.dev.yml",
    "PGPASSWORD": "set by docker-compose.dev.yml",
    "APP_DB_PASSWORD": "generated into .dev/env by `make dev-up`",
    "TENANT": "generated into .dev/env by `make dev-up`",
    "BOOTSTRAP_ADMIN_TOKEN": "generated into .dev/env by `make dev-up`",
    "CONTROL_TOKEN": "generated into .dev/env by `make dev-up`",
}


def _env(name):
    """Required environment, with the remedy attached to the failure."""
    value = os.environ.get(name)
    if value:
        return value
    raise SystemExit(
        "%s is not set (%s).\n"
        "  If you ran `docker compose ... up` directly, use `make dev-up` instead:\n"
        "  compose interpolates ${%s} from the shell, not from env_file, so it\n"
        "  silently becomes an empty string and the failure surfaces minutes later\n"
        "  somewhere else." % (name, _ENV_SOURCE.get(name, "see docs/LOCAL.md"), name))


def _migration_dsn():
    """Prefer a complete DSN, otherwise compose one from parts.

    Composing it here rather than in a shell wrapper is what lets the image be
    distroless: there is no `sh` in it to interpolate the password into a URL.
    """
    dsn = os.environ.get("MIGRATION_DATABASE_URL")
    if dsn:
        return dsn
    host = _env("DBHOST")
    password = urllib.parse.quote(_env("PGPASSWORD"), safe="")
    user = os.environ.get("DBUSER", "switchboard_owner")
    name = os.environ.get("DBNAME", "switchboard")
    ssl = os.environ.get("DBSSLMODE", "verify-full")
    root = os.environ.get("DBSSLROOTCERT", "/etc/ssl/rds/global-bundle.pem")
    return ("postgresql://%s:%s@%s:5432/%s?sslmode=%s&sslrootcert=%s"
            % (user, password, host, name, ssl, root))


def cmd_dbinit(args):
    import psycopg
    from psycopg import sql

    admin = _migration_dsn()
    # Migrations must run as an owner that can create roles and tables; the
    # runtime login is deliberately a different, weaker principal.
    subprocess.run([sys.executable, "-m", "controlplane.migrate"], check=True,
                   env={**os.environ, "MIGRATION_DATABASE_URL": admin})

    password = _env("APP_DB_PASSWORD")
    with psycopg.connect(admin, autocommit=True) as db:
        exists = db.execute("SELECT 1 FROM pg_roles WHERE rolname='switchboard_rt'").fetchone()
        # Role DDL takes no bind parameters, so the password is quoted as a
        # literal by psycopg.sql rather than interpolated by hand.
        secret = sql.Literal(password)
        if exists is None:
            db.execute(sql.SQL("CREATE ROLE switchboard_rt LOGIN PASSWORD {}").format(secret))
        else:
            db.execute(sql.SQL("ALTER ROLE switchboard_rt PASSWORD {}").format(secret))
        # switchboard_app carries the table grants and is NOLOGIN by design.
        db.execute("GRANT switchboard_app TO switchboard_rt")
        db.execute("GRANT CONNECT ON DATABASE switchboard TO switchboard_rt")
        db.execute("GRANT USAGE ON SCHEMA public TO switchboard_rt")
    print("dbinit: migrations applied; switchboard_rt granted switchboard_app")


def _request(method, url, token, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", "Bearer " + token)
    if data:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=10) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def _wait_for(url, attempts=60):
    """Wait for a service, saying so while it waits.

    This could take three minutes (60 attempts x 1s sleep + up to 2s timeout) and
    printed nothing at all until it gave up, so the common case -- the control
    plane crash-looping on an empty APP_DB_PASSWORD -- looked like a hang and
    then reported a timeout that named the symptom rather than the cause.
    """
    last = None
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                if r.status == 200:
                    if attempt > 1:
                        print("  ready after %ds" % attempt)
                    return
                last = "status %d" % r.status
        except Exception as exc:
            last = type(exc).__name__ + ": " + str(exc)
        if attempt == 1 or attempt % 10 == 0:
            print("  waiting for %s (%ds, last: %s)" % (url, attempt, last), flush=True)
        time.sleep(1)
    raise SystemExit(
        "timed out after %ds waiting for %s\n"
        "  last attempt: %s\n"
        "  Check `docker compose -f docker-compose.dev.yml logs controlplane`.\n"
        "  The usual cause is an empty APP_DB_PASSWORD, which happens when compose\n"
        "  is run directly instead of through `make dev-up`." % (attempts, url, last))


def _principal_exists(digest):
    import psycopg
    try:
        dsn = _migration_dsn()
    except (KeyError, SystemExit):
        # No database reachable from here; fall back to asking the API.
        return False
    with psycopg.connect(dsn) as db:
        row = db.execute(
            "SELECT 1 FROM principals WHERE token_hash=%s AND NOT revoked", (digest,)
        ).fetchone()
        return row is not None


def cmd_init(args):
    control = os.environ.get("CONTROL_URL", "http://127.0.0.1:8000")
    tenant = _env("TENANT")
    admin_token = _env("BOOTSTRAP_ADMIN_TOKEN")
    control_token = _env("CONTROL_TOKEN")

    _wait_for(control + "/healthz")

    # Tenant + a seven-day admin principal. Idempotent across reruns: a second
    # run hits the tenants primary key, which is not an error worth failing on.
    provision = subprocess.run(
        [sys.executable, "-m", "scripts.bootstrap", tenant],
        env=os.environ.copy(), capture_output=True, text=True)
    if provision.returncode != 0 and "duplicate key" not in provision.stderr:
        sys.stderr.write(provision.stderr)
        raise SystemExit("tenant provisioning failed")

    # The API stores only the hash; the raw token stays on this side.
    digest = hashlib.sha256(control_token.encode()).hexdigest()
    # token_hash is unique, and the control plane collapses every unhandled
    # exception into an opaque 500, so a duplicate insert is indistinguishable
    # from a real fault over HTTP. Check the table directly instead of trying to
    # read intent out of the status code. (The status was 503 until that was
    # corrected: an unhandled fault is not a retryable one.)
    if _principal_exists(digest):
        print("init: agent principal already present")
    else:
        try:
            agent = _request("POST", control + "/v1/principals", admin_token, {
                "token_hash": digest,
                "role": "agent",
                "expires_at": int(time.time()) + 7 * 86400,
            })
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                # The commonest rerun failure, and it used to be a raw traceback.
                # scripts/bootstrap.py gives the admin principal a seven-day
                # expiry, so a stack left up over a week fails here with exactly
                # the same 401 as a regenerated .dev/env against a kept volume.
                raise SystemExit(
                    "the control plane rejected BOOTSTRAP_ADMIN_TOKEN (401).\n"
                    "  Either .dev/env was regenerated while the postgres volume was kept,\n"
                    "  so the token no longer matches the stored hash, or the bootstrap\n"
                    "  admin principal has passed its seven-day expiry.\n"
                    "  Both are fixed by starting clean: `make dev-down && make dev-up`.")
            raise SystemExit("creating the agent principal failed: HTTP %d %s"
                             % (exc.code, exc.read().decode()[:300]))
        if not agent or "id" not in agent:
            raise SystemExit(
                "the control plane accepted the agent principal but returned no id; "
                "check `docker compose -f docker-compose.dev.yml logs controlplane`")
        print("init: agent principal %s" % agent["id"])

    now = int(time.time())
    policy = {
        "schema": 1,
        "tenant": tenant,
        "version": int(os.environ.get("POLICY_VERSION", "1")),
        "issued_at": now,
        "expires_at": now + 3600,
        "routes": [{"provider": "openai", "model": "mock-model"}],
    }
    try:
        envelope = _request("PUT", control + "/v1/policy", admin_token, policy)
        print("init: policy v%d signed by %s" % (policy["version"], envelope["key_id"]))
    except urllib.error.HTTPError as e:
        if e.code != 409:
            sys.stderr.write(e.read().decode() + "\n")
            raise
        print("init: policy version already published")

    config = {
        "listen": "127.0.0.1:8080",
        "tenant": tenant,
        "data_dir": "/data",
        "control_url": control,
        "control_token_env": "CONTROL_TOKEN",
        "local_token_env": "LOCAL_TOKEN",
        "trusted_keys": {os.environ["POLICY_KEY_ID"]: os.environ["POLICY_PUBLIC_KEY"]},
        # All three at the same mock, which speaks all three request shapes on
        # different paths -- so each exercises its own adapter and its own
        # response parser rather than three names for one code path. The fourth
        # entry is the always-503 mock, which exists so a failover can be
        # demonstrated on demand instead of waited for.
        "providers": {
            "openai": {"url": os.environ.get("MOCK_URL", "http://127.0.0.1:9090"),
                       "key_env": "OPENAI_API_KEY"},
            "anthropic": {"url": os.environ.get("MOCK_URL", "http://127.0.0.1:9090"),
                          "key_env": "ANTHROPIC_API_KEY"},
            "gemini": {"url": os.environ.get("BROKEN_MOCK_URL", "http://127.0.0.1:9091"),
                       "key_env": "GEMINI_API_KEY"},
        },
        "concurrency": 32,
        "rate": 50,
        "burst": 100,
        "retry_rate": 5,
        "max_attempts": 3,
        "timeout_seconds": 60,
        "queue_size": 4096,
        "spool_bytes": 67108864,
        # Empty by default: OTLP export is opt-in, and a dev stack that
        # silently shipped traces somewhere would be a surprise. Set OTLP_URL,
        # OTLP_METRICS_URL and OTLP_HEADERS in .dev/env to point it at a real
        # backend — OTLP_HEADERS maps a header name to the name of the
        # environment variable holding its value, never to the value itself.
        "otlp_url": os.environ.get("OTLP_URL", ""),
        "otlp_metrics_url": os.environ.get("OTLP_METRICS_URL", ""),
        "otlp_headers": json.loads(os.environ.get("OTLP_HEADERS", "{}")),
        # Zero means off, and off is the default here for the same reason it is
        # the default in the product: capture writes prompts and completions to
        # the data directory, and a dev stack that started doing that because
        # someone pulled would be exactly the surprise the flag exists to avoid.
        # Set CAPTURE_TTL_SECONDS in .dev/env to try replay.
        "capture_ttl_seconds": int(os.environ.get("CAPTURE_TTL_SECONDS", "0")),
        "capture_bytes": int(os.environ.get("CAPTURE_BYTES", str(64 << 20))),
        # Required for plain http to loopback; the gateway refuses it otherwise.
        "allow_local_http": True,
        # Local only. pprof exposes memory contents, including provider keys and
        # prompt text; this stack already keeps its signing seed and tokens in
        # plaintext, so it is the right place for it and the wrong place for
        # anything real.
        "enable_pprof": True,
    }
    with open(args.config, "w") as f:
        json.dump(config, f, indent=2)
        f.write("\n")
    print("init: wrote %s" % args.config)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    k = sub.add_parser("keys", help="generate signing material and tokens")
    k.add_argument("--key-id", default="dev-local")
    k.add_argument("--tenant", default="dev-tenant")
    k.set_defaults(func=cmd_keys)

    d = sub.add_parser("dbinit", help="apply migrations and create the runtime login")
    d.set_defaults(func=cmd_dbinit)

    i = sub.add_parser("init", help="provision tenant, principals, policy and config")
    i.add_argument("--config", default="/config/config.json")
    i.set_defaults(func=cmd_init)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
