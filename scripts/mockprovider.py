"""Local stand-in for OpenAI, Anthropic and Gemini.

Speaks the subset of each provider's wire format that internal/gateway/adapter.go
normalizes, streaming and nonstreaming. It exists so the gateway can be exercised
end to end without provider credentials, network egress or per-token cost, and so
retry, circuit-breaker and failover paths can be driven deterministically.

Fault injection, by environment variable:
  MOCK_STATUS        return this HTTP status instead of a completion (e.g. 429, 503)
  MOCK_FAIL_FIRST    fail this many requests with MOCK_STATUS, then succeed
  MOCK_DELAY_MS      sleep before responding, to exercise timeouts and deadlines
  MOCK_TEXT          completion text to return (default: a fixed marker string)

The model each response says served it, per dialect, in that provider's own field
(OpenAI and Anthropic "model", Gemini "modelVersion"):
  MOCK_SERVED_OPENAI, MOCK_SERVED_ANTHROPIC, MOCK_SERVED_GEMINI
                     "echo" (the default) names the model the request asked for,
                     "omit" leaves the field out, and anything else is sent as is
  MOCK_CONTROL=1     enables POST /control {"dialect": ..., "served_model": ...} to
                     change one of those while running, and GET /control to read
                     them. This is how a provider repointing an alias at a new
                     snapshot is simulated for the drift demo: the mock really
                     starts answering as a different model, and nothing downstream
                     is told. Off unless set, so nothing can flip it by accident.

Standard library only; it runs on a bare python image with nothing installed.
"""
import json
import os
import re
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TEXT = os.environ.get("MOCK_TEXT", "switchboard mock provider reached")
DELAY_MS = int(os.environ.get("MOCK_DELAY_MS", "0"))
STATUS = int(os.environ.get("MOCK_STATUS", "0"))
FAIL_FIRST = int(os.environ.get("MOCK_FAIL_FIRST", "0"))
CONTROL = os.environ.get("MOCK_CONTROL") == "1"

_lock = threading.Lock()
_seen = 0

# The served model per dialect. Guarded by _lock: /control writes it while
# completions on other threads read it.
_served = {d: os.environ.get("MOCK_SERVED_" + d.upper(), "echo") for d in ("openai", "anthropic", "gemini")}

# What a control request may set a served model to, beyond echo and omit: a
# plausible model identifier, so the header and dashboards it ends up in stay
# readable, and nothing with a separator in it reaches them.
_MODEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


def _should_fail():
    """Fail while inside the MOCK_FAIL_FIRST window, or always if MOCK_STATUS alone is set."""
    global _seen
    if STATUS == 0:
        return False
    if FAIL_FIRST == 0:
        return True
    with _lock:
        _seen += 1
        return _seen <= FAIL_FIRST


def served_model(dialect, requested):
    """The model a response in this dialect names, or None to leave the field out."""
    with _lock:
        mode = _served[dialect]
    if mode == "omit":
        return None
    if mode == "echo":
        return requested or None
    return mode


def _named(obj, key, model):
    if model is not None:
        obj[key] = model
    return obj


def _sse(chunks, done=True):
    return "".join("data: " + json.dumps(c) + "\n\n" for c in chunks) + ("data: [DONE]\n\n" if done else "")


def openai_body(stream, model):
    if not stream:
        return _named({
            "choices": [{"index": 0, "message": {"content": TEXT}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 11, "completion_tokens": 7},
        }, "model", model)
    head, tail = TEXT[: len(TEXT) // 2], TEXT[len(TEXT) // 2 :]
    # OpenAI names the model on every chunk.
    return _sse([_named(c, "model", model) for c in (
        {"choices": [{"index": 0, "delta": {"content": head}, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {"content": tail}, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
         "usage": {"prompt_tokens": 11, "completion_tokens": 7}},
    )])


def anthropic_body(stream, model):
    if not stream:
        return _named({
            "content": [{"type": "text", "text": TEXT}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 11, "output_tokens": 7},
        }, "model", model)
    head, tail = TEXT[: len(TEXT) // 2], TEXT[len(TEXT) // 2 :]
    # Anthropic names the model once, in message_start, and ends with
    # message_stop rather than [DONE]. The gateway treats [DONE] from anything but
    # OpenAI as a stream cut short, so a stream ending that way failed.
    return _sse([
        {"type": "message_start",
         "message": _named({"type": "message", "role": "assistant", "content": []}, "model", model)},
        {"type": "content_block_start", "content_block": {"type": "text", "text": ""}},
        {"type": "content_block_delta", "delta": {"type": "text_delta", "text": head}},
        {"type": "content_block_delta", "delta": {"type": "text_delta", "text": tail}},
        {"type": "message_delta", "delta": {"stop_reason": "end_turn"},
         "usage": {"input_tokens": 11, "output_tokens": 7}},
        {"type": "message_stop"},
    ], done=False)


def gemini_body(stream, model):
    def response(text, finish):
        # modelVersion is a field of the response, not of a candidate.
        return _named({"candidates": [{"index": 0, "content": {"parts": [{"text": text}]},
                                       "finishReason": finish}],
                       "usageMetadata": {"promptTokenCount": 11, "candidatesTokenCount": 7}},
                      "modelVersion", model)
    if not stream:
        return response(TEXT, "STOP")
    head, tail = TEXT[: len(TEXT) // 2], TEXT[len(TEXT) // 2 :]
    return _sse([response(head, ""), response(tail, "STOP")])


GEMINI = re.compile(r"^/v1beta/models/([^/:]+):(generateContent|streamGenerateContent)$")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("mockprovider %s\n" % (fmt % args))

    def _send(self, status, body, content_type="application/json"):
        raw = body if isinstance(body, bytes) else (
            body if isinstance(body, str) else json.dumps(body)).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        # Unauthenticated liveness probe for compose health checks.
        if self.path == "/healthz":
            return self._send(200, {"ok": True})
        if self.path == "/control" and CONTROL:
            with _lock:
                state = dict(_served)
            return self._send(200, state)
        self._send(404, {"error": "not found"})

    def _control(self):
        """Change what one dialect says served it. Handled before delay and failure
        injection, so a change scheduled during a simulated outage still lands."""
        length = int(self.headers.get("Content-Length") or 0)
        if length > 1024:
            # The body is not read, so this connection cannot be reused.
            self.close_connection = True
            return self._send(413, {"error": "a control body is limited to 1 KB"})
        try:
            body = json.loads(self.rfile.read(length) or b"null")
        except ValueError:
            return self._send(400, {"error": "invalid json"})
        if not isinstance(body, dict) or set(body) != {"dialect", "served_model"}:
            return self._send(400, {"error": 'the body must be exactly {"dialect": ..., "served_model": ...}'})
        dialect, model = body["dialect"], body["served_model"]
        if dialect not in _served:
            return self._send(400, {"error": "dialect must be one of " + ", ".join(sorted(_served))})
        if not isinstance(model, str) or not (model in ("echo", "omit") or _MODEL.match(model)):
            return self._send(400, {"error": 'served_model must be "echo", "omit" or a model identifier'})
        with _lock:
            previous = _served[dialect]
            _served[dialect] = model
        sys.stderr.write("mockprovider %s now answers as %s (was %s)\n" % (dialect, model, previous))
        return self._send(200, {"dialect": dialect, "served_model": model,
                                "previous": previous, "effective_at": time.time()})

    def do_POST(self):
        if self.path.split("?")[0] == "/control":
            if CONTROL:
                return self._control()
            self.close_connection = True
            return self._send(404, {"error": "not found"})

        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            req = json.loads(raw or b"{}")
        except ValueError:
            return self._send(400, {"error": "invalid json"})

        if DELAY_MS:
            time.sleep(DELAY_MS / 1000.0)
        if _should_fail():
            return self._send(STATUS, {"error": {"type": "mock_injected", "status": STATUS}})

        path = self.path.split("?")[0]
        requested = req.get("model") if isinstance(req, dict) and isinstance(req.get("model"), str) else None
        if path == "/v1/chat/completions":
            stream = bool(req.get("stream"))
            body = openai_body(stream, served_model("openai", requested))
        elif path == "/v1/messages":
            stream = bool(req.get("stream"))
            body = anthropic_body(stream, served_model("anthropic", requested))
        elif GEMINI.match(path):
            m = GEMINI.match(path)
            stream = m.group(2) == "streamGenerateContent"
            body = gemini_body(stream, served_model("gemini", m.group(1)))
        else:
            return self._send(404, {"error": "unsupported path: " + path})

        if isinstance(body, str):
            return self._send(200, body, "text/event-stream")
        self._send(200, body)


def main():
    host = os.environ.get("MOCK_HOST", "127.0.0.1")
    port = int(os.environ.get("MOCK_PORT", "9090"))
    server = ThreadingHTTPServer((host, port), Handler)
    sys.stderr.write("mockprovider listening on %s:%d\n" % (host, port))
    sys.stderr.flush()
    server.serve_forever()


if __name__ == "__main__":
    main()
