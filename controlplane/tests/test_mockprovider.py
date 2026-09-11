"""The dev mock's served-model knob, which the drift demo rests on.

dev-traffic simulates a provider repointing an alias at a new snapshot by telling
the mock to answer as a different model. That is only honest if the mock really
does answer differently, in each provider's own field, and if nothing but a
control endpoint that was deliberately enabled can change it.
"""
import http.client
import importlib.util
import json
import os
import pathlib
import threading
import unittest
from http.server import ThreadingHTTPServer

_MOCK = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "mockprovider.py"
_KNOBS = ("MOCK_CONTROL", "MOCK_STATUS", "MOCK_FAIL_FIRST", "MOCK_DELAY_MS", "MOCK_TEXT",
          "MOCK_SERVED_OPENAI", "MOCK_SERVED_ANTHROPIC", "MOCK_SERVED_GEMINI")


class MockServedModel(unittest.TestCase):
    def start(self, **env):
        """Load mockprovider.py by path with these knobs -- it reads them at import -- and serve it."""
        saved = {k: os.environ.get(k) for k in _KNOBS}
        for k in _KNOBS:
            os.environ.pop(k, None)
        os.environ.update(env)
        try:
            spec = importlib.util.spec_from_file_location("mockprovider_under_test", _MOCK)
            mock = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mock)
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        server = ThreadingHTTPServer(("127.0.0.1", 0), mock.Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.port = server.server_address[1]

    def request(self, method, path, body=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        self.addCleanup(conn.close)
        raw = None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
        conn.request(method, path, body=raw, headers={"Content-Type": "application/json"})
        res = conn.getresponse()
        return res.status, res.getheader("Content-Type", ""), res.read().decode()

    def complete(self, dialect, model="gpt-4o-mini", stream=False):
        """One completion in the dialect's own request shape: the body, or a stream's frames."""
        if dialect == "gemini":
            path = "/v1beta/models/%s:%s" % (model, "streamGenerateContent" if stream else "generateContent")
            body = {"contents": []}
        else:
            path = "/v1/chat/completions" if dialect == "openai" else "/v1/messages"
            body = {"model": model, "stream": stream}
        status, _, data = self.request("POST", path, body)
        self.assertEqual(status, 200, data)
        if not stream:
            return [json.loads(data)]
        frames = [line[len("data: "):] for line in data.split("\n") if line.startswith("data: ")]
        return [json.loads(f) for f in frames if f != "[DONE]"]

    def test_echo_names_the_requested_model_in_each_dialects_own_field(self):
        self.start()
        self.assertEqual(self.complete("openai")[0]["model"], "gpt-4o-mini")
        for frame in self.complete("openai", stream=True):
            self.assertEqual(frame["model"], "gpt-4o-mini", "OpenAI names the model on every chunk")
        self.assertEqual(self.complete("anthropic", model="claude-sonnet-4")[0]["model"], "claude-sonnet-4")
        frames = self.complete("anthropic", model="claude-sonnet-4", stream=True)
        self.assertEqual(frames[0]["type"], "message_start")
        self.assertEqual(frames[0]["message"]["model"], "claude-sonnet-4")
        self.assertEqual(self.complete("gemini", model="gemini-2.0-flash")[0]["modelVersion"], "gemini-2.0-flash")
        for frame in self.complete("gemini", model="gemini-2.0-flash", stream=True):
            self.assertEqual(frame["modelVersion"], "gemini-2.0-flash")

    def test_an_anthropic_stream_ends_the_way_the_gateway_requires(self):
        # message_stop and no [DONE]. The gateway treats [DONE] from anything but
        # OpenAI as a premature end, so a mock stream that ended that way failed.
        self.start()
        status, ctype, data = self.request("POST", "/v1/messages", {"model": "claude-sonnet-4", "stream": True})
        self.assertEqual(status, 200)
        self.assertEqual(ctype, "text/event-stream")
        self.assertNotIn("[DONE]", data)
        frames = [json.loads(line[6:]) for line in data.split("\n") if line.startswith("data: ")]
        self.assertEqual(frames[-1]["type"], "message_stop")

    def test_omit_leaves_the_field_out_rather_than_null(self):
        self.start(MOCK_SERVED_OPENAI="omit", MOCK_SERVED_ANTHROPIC="omit", MOCK_SERVED_GEMINI="omit")
        self.assertNotIn("model", self.complete("openai")[0])
        self.assertNotIn("model", self.complete("anthropic")[0])
        self.assertNotIn("model", self.complete("anthropic", stream=True)[0]["message"])
        self.assertNotIn("modelVersion", self.complete("gemini")[0])

    def test_a_literal_is_sent_whatever_was_asked_for(self):
        self.start(MOCK_SERVED_OPENAI="gpt-4o-mini-2024-07-18")
        self.assertEqual(self.complete("openai", model="gpt-4o-mini")[0]["model"], "gpt-4o-mini-2024-07-18")

    def test_control_changes_one_dialect_and_says_what_it_was(self):
        self.start(MOCK_CONTROL="1")
        status, _, data = self.request("POST", "/control",
                                       {"dialect": "openai", "served_model": "gpt-4o-mini-simulated-snapshot"})
        self.assertEqual(status, 200, data)
        reply = json.loads(data)
        self.assertEqual((reply["dialect"], reply["served_model"], reply["previous"]),
                         ("openai", "gpt-4o-mini-simulated-snapshot", "echo"))
        self.assertIsInstance(reply["effective_at"], float)
        self.assertEqual(self.complete("openai")[0]["model"], "gpt-4o-mini-simulated-snapshot")
        self.assertEqual(self.complete("anthropic", model="claude-sonnet-4")[0]["model"], "claude-sonnet-4",
                         "only the named dialect changes")
        _, _, data = self.request("GET", "/control")
        self.assertEqual(json.loads(data),
                         {"openai": "gpt-4o-mini-simulated-snapshot", "anthropic": "echo", "gemini": "echo"})

    def test_control_answers_even_while_failures_are_injected(self):
        # Handled before MOCK_STATUS: a change scheduled during a simulated outage
        # has to land, or the outage and the drift cannot be shown together.
        self.start(MOCK_CONTROL="1", MOCK_STATUS="503")
        status, _, _ = self.request("POST", "/control", {"dialect": "gemini", "served_model": "omit"})
        self.assertEqual(status, 200)

    def test_control_is_absent_unless_enabled(self):
        self.start()
        self.assertEqual(self.request("POST", "/control", {"dialect": "openai", "served_model": "x"})[0], 404)
        self.assertEqual(self.request("GET", "/control")[0], 404)

    def test_control_refuses_what_it_cannot_honestly_apply(self):
        self.start(MOCK_CONTROL="1")
        for body in (
            {"dialect": "openai"},
            {"dialect": "openai", "served_model": "x", "extra": 1},
            {"dialect": "bedrock", "served_model": "x"},
            {"dialect": "openai", "served_model": "not a model id"},
            {"dialect": "openai", "served_model": 4},
            b"not json",
        ):
            status, _, data = self.request("POST", "/control", body)
            self.assertEqual(status, 400, "%r -> %s %s" % (body, status, data))
        self.assertEqual(self.request("POST", "/control", b"{" + b" " * 2048 + b"}")[0], 413)
        _, _, data = self.request("GET", "/control")
        self.assertEqual(json.loads(data)["openai"], "echo", "a refused change must not half-apply")


if __name__ == "__main__":
    unittest.main()
