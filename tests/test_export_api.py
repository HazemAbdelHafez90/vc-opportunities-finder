import hashlib
import os
import sys
import types
import unittest
from unittest.mock import patch

bs4_stub = types.ModuleType("bs4")
bs4_stub.BeautifulSoup = object
sys.modules.setdefault("bs4", bs4_stub)

from api._export import ExportError, authenticate_export_token, build_query


class ExportAuthTests(unittest.TestCase):
    def test_missing_or_wrong_token_is_rejected(self):
        digest = hashlib.sha256(b"good").hexdigest()
        with patch.dict(os.environ, {"EXPORT_API_TOKEN_SHA256S": digest}):
            with self.assertRaises(PermissionError): authenticate_export_token({})
            with self.assertRaises(PermissionError): authenticate_export_token({"Authorization": "Bearer wrong"})

    def test_two_tokens_can_be_valid_during_rotation(self):
        first = hashlib.sha256(b"first").hexdigest()
        second = hashlib.sha256(b"second").hexdigest()
        with patch.dict(os.environ, {"EXPORT_API_TOKEN_SHA256S": f"{first},{second}"}):
            self.assertEqual(authenticate_export_token({"Authorization": "Bearer first"}), 0)
            self.assertEqual(authenticate_export_token({"Authorization": "Bearer second"}), 1)

    def test_unset_token_configuration_disables_export(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError): authenticate_export_token({"Authorization": "Bearer anything"})


class ExportQueryTests(unittest.TestCase):
    def test_defaults_include_every_status_and_use_stable_page_order(self):
        query, raw, limit, offset = build_query({})
        self.assertNotIn("status", query)
        self.assertEqual(query["order"], "first_seen_at.desc,id.asc")
        self.assertFalse(raw)
        self.assertEqual((limit, offset), (200, 0))

    def test_filters_and_raw_export_are_requested(self):
        query, raw, limit, offset = build_query({"status": ["expired,stale"], "source": ["UNGM"], "fit": ["High fit"], "q": ["video"], "include_raw": ["1"], "limit": ["500"], "offset": ["5"]})
        self.assertEqual(query["status"], "in.(expired,stale)")
        self.assertEqual(query["source"], "in.(UNGM)")
        self.assertEqual(query["fit_label"], "eq.High fit")
        self.assertEqual(query["title"], "ilike.*video*")
        self.assertIn("raw_payload", query["select"])
        self.assertTrue(raw)
        self.assertEqual((limit, offset), (500, 5))

    def test_bad_parameters_are_rejected(self):
        for params in ({"status": ["deleted"]}, {"limit": ["501"]}, {"deadline_from": ["not-a-date"]}, {"include_raw": ["yes"]}):
            with self.subTest(params=params):
                with self.assertRaises(ExportError): build_query(params)


if __name__ == "__main__":
    unittest.main()
