#!/usr/bin/env python3
"""A prompt console for the local stack: send a prompt, see the route it took.

Dev-stack only, and deliberately not part of the product. It exists because of a
topology fact rather than a preference: the gateway listens on 127.0.0.1 and
Docker cannot publish a container's loopback, so a browser on the host can never
reach it. The control plane can -- they share a network namespace -- but the
control plane signs policies and receives telemetry and is deliberately not in
the request path, and putting it there to make a demo work would trade a real
architectural property for a convenience.

So: this joins the same namespace, reaches the gateway on localhost exactly as
the sidecar topology intends, and serves a page on 0.0.0.0 that a browser can
reach.

  CONSOLE_PORT   where to listen                      (default 8090)
  GATEWAY_URL    the gateway, on the shared namespace (default http://127.0.0.1:8080)
  LOCAL_TOKEN    the caller token the gateway expects
"""

import json
import os
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PORT = int(os.environ.get("CONSOLE_PORT", "8090"))
GATEWAY = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8080").rstrip("/")
TOKEN = os.environ.get("LOCAL_TOKEN", "")
PAGE = Path(__file__).with_name("console.html")

# The headers the page draws the route from. Listed rather than forwarded
# wholesale so that adding a header to the gateway cannot silently start
# leaking something through this proxy.
FORWARD = [
    "X-Request-ID",
    "X-Switchboard-Provider",
    "X-Switchboard-Attempts",
    "X-Switchboard-Route",
    "X-Switchboard-Replayed",
    "X-Switchboard-Prompt-Cache",
    "traceparent",
]


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass  # the gateway logs the request; this would only double it

    def _send(self, status, body, ctype="application/json", extra=None):
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path.split("?")[0] in ("/", "/console"):
            return self._send(200, PAGE.read_bytes(), "text/html; charset=utf-8")
        return self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path.split("?")[0] != "/send":
            return self._send(404, {"error": "not found"})
        try:
            n = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return self._send(400, {"error": "bad length"})
        if n > 1 << 20:
            return self._send(413, {"error": "too large"})
        body = self.rfile.read(n)

        req = urllib.request.Request(
            GATEWAY + "/v1/chat/completions", data=body,
            headers={"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                out, status, headers = r.read(), r.status, r.headers
        except urllib.error.HTTPError as e:
            # A refusal is a result, not an error: the whole point of the page is
            # to show what the gateway decided, and deciding not to route is a
            # decision. Its headers matter as much as a success's.
            out, status, headers = e.read(), e.code, e.headers
        except Exception as e:
            return self._send(502, {"error": "the gateway could not be reached: %s" % e})

        # Handed to the page as data rather than as headers on this response, so
        # the page reads one shape whether the gateway answered or refused.
        return self._send(200, {
            "status": status,
            "headers": {k: headers.get(k) for k in FORWARD if headers.get(k)},
            "body": out.decode("utf-8", "replace"),
        })


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("LOCAL_TOKEN is required: the gateway will refuse every request without it")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
