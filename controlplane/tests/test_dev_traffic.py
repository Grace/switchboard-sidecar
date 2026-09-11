"""The piece of dev-traffic.py its drift self-check rests on.

The script drives real traffic and is exercised by running it against the dev
stack. What is pinned here is how it reads the served model out of
X-Switchboard-Route -- the same grammar console.html parses -- because a
self-check that misreads the header passes a demo that did not work.
"""
import importlib.util
import os
import pathlib
import unittest

_SCRIPT = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "dev-traffic.py"


def _load():
    """Import dev-traffic.py by path; it reads its tokens from the environment at import."""
    env = {"BOOTSTRAP_ADMIN_TOKEN": "test", "LOCAL_TOKEN": "test", "TENANT": "tenant-test"}
    saved = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        spec = importlib.util.spec_from_file_location("dev_traffic_under_test", _SCRIPT)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class ServedSuffix(unittest.TestCase):
    def setUp(self):
        self.served = _load().served_suffix

    def test_served_as_sent_is_empty(self):
        self.assertEqual(self.served("openai/gpt-4o-mini:200"), "")

    def test_a_different_served_model_is_returned(self):
        self.assertEqual(self.served("openai/gpt-4o-mini=gpt-4o-mini-simulated-snapshot:200"),
                         "gpt-4o-mini-simulated-snapshot")

    def test_an_unstated_model_is_a_question_mark(self):
        self.assertEqual(self.served("anthropic/claude-sonnet-4=?:200"), "?")

    def test_only_the_answering_hop_counts(self):
        self.assertEqual(self.served("gemini/gemini-2.0-flash:skipped circuit_open, openai/gpt-4o-mini=snap:200"),
                         "snap")
        self.assertEqual(self.served("openai/gpt-5-nano=gpt-5-nano-2025-08-07:200 empty_completion, "
                                     "anthropic/claude-sonnet-4:200"), "")

    def test_a_model_id_containing_a_colon(self):
        self.assertEqual(self.served("bedrock/anthropic.claude-3-5-haiku-20241022-v1:0=?:200"), "?")
        self.assertEqual(self.served("bedrock/anthropic.claude-3-5-haiku-20241022-v1:0:200"), "")

    def test_nothing_to_read(self):
        self.assertIsNone(self.served(""))
        self.assertIsNone(self.served("not a route header"))

    def test_the_injected_model_says_it_is_simulated(self):
        # The word has to be in the data, not only in a label beside it.
        self.assertIn("simulated", _load().DRIFT["flip_to"])


if __name__ == "__main__":
    unittest.main()
