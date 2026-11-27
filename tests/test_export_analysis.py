import importlib
import io
import json
import unittest
from unittest.mock import patch

export = importlib.import_module("api.export-analysis")


class ExportTests(unittest.TestCase):
    def test_paginates_and_preserves_document_data(self):
        first = [{"id": str(i), "raw_payload": {"documents": [{"text": "scope"}]}} for i in range(500)]
        with patch.object(export, "supabase_request", side_effect=[first, [{"id": "last"}], []]) as db:
            result = export.build_export()
        self.assertEqual(result["opportunity_count"], 501)
        self.assertEqual(result["opportunities"][0]["raw_payload"]["documents"][0]["text"], "scope")
        self.assertEqual(db.call_args_list[1].kwargs["query"]["offset"], "500")
        self.assertNotIn("reporter_email", db.call_args_list[2].kwargs["query"]["select"])

    def test_unauthenticated_request_does_not_read_database(self):
        request = object.__new__(export.handler)
        request.headers = {}
        request.wfile = io.BytesIO()
        request.send_response = lambda status: setattr(request, "status", status)
        request.send_header = lambda *args: None
        request.end_headers = lambda: None
        with patch.object(export, "get_authenticated_user", side_effect=export.AuthError("no")), patch.object(export, "build_export") as build:
            request.do_GET()
        self.assertEqual(request.status, 401)
        self.assertEqual(json.loads(request.wfile.getvalue())["error"], "Authentication required.")
        build.assert_not_called()
