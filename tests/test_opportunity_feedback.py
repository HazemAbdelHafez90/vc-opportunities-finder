import importlib
import io
import json
import unittest
from unittest.mock import patch

feedback = importlib.import_module("api.opportunity-feedback")


class FeedbackTests(unittest.TestCase):
    def request(self, payload):
        handler = object.__new__(feedback.handler)
        raw = json.dumps(payload).encode()
        handler.headers = {"Content-Length": str(len(raw))}
        handler.rfile = io.BytesIO(raw)
        handler.write_json = lambda body, status=200: setattr(handler, "result", (status, body))
        return handler

    @patch.object(feedback, "supabase_request")
    @patch.object(feedback, "get_authenticated_user", return_value={"email": "ops@example.org"})
    def test_report_records_identity_and_comment_without_changing_opportunity(self, auth, database):
        handler = self.request({"id": "opportunity-id", "comment": "Equipment purchase"})
        handler.do_POST()
        self.assertEqual(handler.result, (200, {"saved": True}))
        database.assert_called_once_with("POST", "opportunity_feedback", payload={
            "opportunity_id": "opportunity-id", "reporter_email": "ops@example.org", "comment": "Equipment purchase",
        }, prefer="return=minimal")

    @patch.object(feedback, "supabase_request")
    @patch.object(feedback, "get_authenticated_user", return_value={"email": "ops@example.org"})
    def test_report_rejects_long_comments(self, auth, database):
        handler = self.request({"id": "opportunity-id", "comment": "a" * 1001})
        handler.do_POST()
        self.assertEqual(handler.result[0], 400)
        database.assert_not_called()


if __name__ == "__main__":
    unittest.main()
