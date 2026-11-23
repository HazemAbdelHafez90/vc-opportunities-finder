import unittest
from unittest.mock import patch
from api._lib import SourceProbe, fetch_eeas_tenders, fetch_fairtrade_opportunities


class CandidateSourceTests(unittest.TestCase):
    def test_eeas_reads_multiple_pages_and_deduplicates(self):
        html = '<article><a href="/eeas/tender-photography_en">Tender for photography services</a><p>Deadline: 31-12-2099</p></article>'
        probe = SourceProbe()
        with patch('api._lib.request_text', return_value=html) as fetch:
            items = fetch_eeas_tenders(['photography'], probe=probe)
        self.assertEqual(fetch.call_count, 5)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]['deadline'], '2099-12-31')
        self.assertEqual(probe.rows_parsed, 1)

    def test_fairtrade_ignores_vacancies_and_handles_empty_tenders(self):
        html = '<h2>Vacancies</h2><ul><li><a href="/photo-job">Photography vacancy</a></li></ul><h2>Tenders from Fairtrade International</h2><p>No current tenders</p><h2>Public Consultations</h2>'
        with patch('api._lib.request_text', return_value=html):
            self.assertEqual(fetch_fairtrade_opportunities(['photography']), [])

    def test_fairtrade_extracts_tenders(self):
        html = '<h2>Tenders from Fairtrade International</h2><ul><li><a href="/photo-tender">Photography services</a> Deadline: 31-12-2099</li></ul><h2>Public Consultations</h2>'
        with patch('api._lib.request_text', return_value=html):
            items = fetch_fairtrade_opportunities(['photography'])
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]['source'], 'Fairtrade International')


if __name__ == '__main__':
    unittest.main()
