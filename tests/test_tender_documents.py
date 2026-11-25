import unittest
from unittest.mock import patch

from lib import tender_documents


class Response:
    def __init__(self, data):
        self.data = data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def read(self, limit):
        return self.data[:limit]


class TenderDocumentTests(unittest.TestCase):
    def test_reads_document_from_same_session(self):
        html = b'<a class="lnkShowDocument" href="/Public/Notice/DownloadDocument?documentId=1">Terms.pdf</a>'

        class Opener:
            addheaders = []

            def open(self, req, timeout):
                if isinstance(req, str):
                    return Response(html)
                self.referer = req.get_header("Referer")
                return Response(b"%PDF-1.7\nexample")

        class Page:
            def extract_text(self):
                return "Submission deadline: 20 November 2026"

        opener = Opener()
        with patch.object(tender_documents.request, "build_opener", return_value=opener), patch.object(
            tender_documents, "PdfReader", return_value=type("PDF", (), {"pages": [Page()]})()
        ):
            result = tender_documents.read_ungm_documents(
                "https://www.ungm.org/Public/Notice/323461", headers={}, ssl_context=None
            )
        self.assertEqual(result[0]["status"], "read")
        self.assertIn("20 November 2026", result[0]["text"])
        self.assertEqual(opener.referer, "https://www.ungm.org/Public/Notice/323461")

    def test_unavailable_notice_does_not_stop_other_items(self):
        items = [{"source": "UNGM", "link": "https://www.ungm.org/Public/Notice/1"}]
        with patch.object(tender_documents, "read_source_documents", side_effect=RuntimeError("403")):
            tender_documents.enrich_ungm_items(items, headers={}, ssl_context=None)
        self.assertEqual(items[0]["documentStatus"], "unavailable")

    def test_rejects_other_hosts(self):
        with self.assertRaises(ValueError):
            tender_documents.read_ungm_documents("https://example.com/Public/Notice/1", headers={}, ssl_context=None)

    def test_extracts_attributed_deadlines_and_requirements_without_question_deadline(self):
        text = (
            "Deadline for submission: 20 November 2026\n"
            "Questions deadline: 15 November 2026\n"
            "Applicants must have at least five years of documentary experience.\n"
        )
        deadlines, requirements = tender_documents.extract_findings([(3, text)], "Terms of reference.pdf", "https://www.cafonline.com/tender.pdf")
        self.assertEqual([item["date"] for item in deadlines], ["2026-11-20"])
        self.assertEqual(deadlines[0]["page"], 3)
        self.assertIn("five years", requirements[0]["excerpt"])

    def test_guidance_documents_do_not_create_tender_requirements(self):
        result = tender_documents.extract_findings(
            [(1, "Bidders must submit a proposal by 20 November 2026.")],
            "UNGM_Bidders_Instructions_EN-FR-ES__2026.pdf", "https://www.ungm.org/x",
        )
        self.assertEqual(result, ([], []))
        self.assertEqual(tender_documents.extract_fit_excerpts(
            [(1, "The bidder must provide photography and visual storytelling.")],
            "UNGM_Bidders_Instructions_EN-FR-ES__2026.pdf"), [])

    def test_fit_excerpts_find_relevant_passages_after_first_excerpt_limit(self):
        text = "Unrelated page text.\n" * 1000 + "The contractor will deliver documentary photography and video production."
        excerpts = tender_documents.extract_fit_excerpts([(7, text)], "Terms of reference.pdf")
        self.assertEqual(excerpts[0]["page"], 7)
        self.assertIn("documentary photography", excerpts[0]["text"])

    def test_ambiguous_dates_and_clarifications_are_not_submission_deadlines(self):
        text = "Submission deadline: 03/04/2026\nDeadline for questions: 15 November 2026"
        deadlines, _ = tender_documents.extract_findings([(1, text)], "RFP.pdf", "https://www.cafonline.com/x")
        self.assertEqual(deadlines, [])

    def test_does_not_override_source_deadline_or_choose_between_conflicts(self):
        document = {"status": "read", "deadlines": [{"date": "2026-11-20"}, {"date": "2026-11-21"}]}
        item = {"documents": [document], "documentStatus": "checked", "deadline": None}
        tender_documents.apply_document_deadline(item)
        self.assertIsNone(item["deadline"])
        document["deadlines"].pop()
        item["deadline"] = "2026-11-30"
        tender_documents.apply_document_deadline(item)
        self.assertEqual(item["deadline"], "2026-11-30")
        item["deadline"] = None
        tender_documents.apply_document_deadline(item)
        self.assertEqual(item["deadline"], "2026-11-20")
        self.assertTrue(item["deadlineFromDocument"])

    def test_interleaves_sources_and_preserves_pending_items(self):
        items = [{"source": "UNGM", "link": "https://www.ungm.org/Public/Notice/1"},
                 {"source": "UNGM", "link": "https://www.ungm.org/Public/Notice/2"},
                 {"source": "CAF", "link": "https://www.cafonline.com/tender"}]
        with patch.object(tender_documents, "read_source_documents", return_value=[]) as reader:
            tender_documents.enrich_documents(items, headers={}, ssl_context=None, max_notices=2)
        self.assertEqual([call.args[0]["source"] for call in reader.call_args_list], ["UNGM", "CAF"])
        self.assertEqual(items[1]["documentStatus"], "pending")


if __name__ == "__main__":
    unittest.main()
