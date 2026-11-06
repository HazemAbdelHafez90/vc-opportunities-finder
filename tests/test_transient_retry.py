import io
import sys
import types
import unittest
from urllib import error

bs4_stub = types.ModuleType("bs4")
bs4_stub.BeautifulSoup = object
sys.modules.setdefault("bs4", bs4_stub)

from api import _lib


class FakeResponse:
    def __init__(self, body: bytes, status: int = 200):
        self._body = body
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return self._body

    def getcode(self):
        return self.status


def http_error(code: int) -> error.HTTPError:
    return error.HTTPError(
        "https://example.test/x", code, "Gateway Timeout", {}, io.BytesIO(b"<html>gateway</html>")
    )


class TransientRetryTests(unittest.TestCase):
    """Find a Tender answered one 504 on the first production run and the whole
    source was marked failed, which opened an incident and emailed three people.
    It answers in under two seconds on every other attempt."""

    def setUp(self):
        self.original_urlopen = _lib.request.urlopen
        self.original_sleep = _lib.time.sleep
        self.slept = []
        _lib.time.sleep = self.slept.append

    def tearDown(self):
        _lib.request.urlopen = self.original_urlopen
        _lib.time.sleep = self.original_sleep

    def responses(self, *outcomes):
        """Each outcome is a status code to raise, or bytes to return."""
        remaining = list(outcomes)
        self.calls = 0

        def fake_urlopen(req, timeout=None, context=None):
            self.calls += 1
            outcome = remaining.pop(0)
            if isinstance(outcome, int):
                raise http_error(outcome)
            return FakeResponse(outcome)

        _lib.request.urlopen = fake_urlopen

    def test_a_blip_is_retried_and_the_probe_keeps_the_final_status(self):
        """The probe keeps the worst status a run saw, so recording the 504 we
        recovered from would raise the incident the retry just avoided."""
        self.responses(504, b'{"releases": []}')
        probe = _lib.SourceProbe()

        body = _lib.request_text("https://example.test/x", probe=probe, retries=2, retry_delay=0)

        self.assertEqual(body, '{"releases": []}')
        self.assertEqual(self.calls, 2)
        self.assertEqual(probe.http_status, 200)

    def test_a_sustained_outage_still_fails(self):
        self.responses(504, 504, 504)
        probe = _lib.SourceProbe()

        with self.assertRaises(RuntimeError):
            _lib.request_text("https://example.test/x", probe=probe, retries=2, retry_delay=0)

        self.assertEqual(self.calls, 3)
        self.assertEqual(probe.http_status, 504)

    def test_an_answer_about_our_request_is_not_retried(self):
        """A 404 or a 400 is the source telling us something; asking again wastes
        seconds of a sync that is already capped at 300."""
        for code in (400, 403, 404, 429):
            with self.subTest(code=code):
                self.responses(code, b"never reached")
                with self.assertRaises(RuntimeError):
                    _lib.request_text("https://example.test/x", retries=2, retry_delay=0)
                self.assertEqual(self.calls, 1)

    def test_retries_are_off_by_default(self):
        """Every other source calls request_text without asking for retries and
        must behave exactly as it did before."""
        self.responses(504, b"never reached")
        with self.assertRaises(RuntimeError):
            _lib.request_text("https://example.test/x")
        self.assertEqual(self.calls, 1)

    def test_the_wait_grows_between_attempts(self):
        self.responses(502, 503, b"{}")
        _lib.request_text("https://example.test/x", retries=2, retry_delay=1.5)
        self.assertEqual(self.slept, [1.5, 3.0])

    def test_find_a_tender_asks_for_retries(self):
        """The regression this guards: the fetcher must pass retries, or a single
        gateway blip fails the source again."""
        seen = {}

        def fake_request_text(url, data=None, headers=None, probe=None, retries=0, retry_delay=2.0):
            seen["retries"] = retries
            return '{"releases": []}'

        original = _lib.request_text
        _lib.request_text = fake_request_text
        try:
            _lib.fetch_find_a_tender_notices(["video"], probe=_lib.SourceProbe())
        finally:
            _lib.request_text = original

        self.assertEqual(seen["retries"], 2)


if __name__ == "__main__":
    unittest.main()
