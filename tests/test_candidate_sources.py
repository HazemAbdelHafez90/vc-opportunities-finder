import unittest
from unittest.mock import patch
from api._lib import SourceProbe, fetch_eeas_tenders, fetch_fairtrade_opportunities


class CandidateSourceTests(unittest.TestCase):
    def test_eeas_reads_multiple_pages_and_deduplicates(self):
        html = '<article><a href="/eeas/tender-photography_en">Tender for photography services</a><p>Deadline: 31-12-2099</p></article>'
        probe = SourceProbe()
        with patch('api._lib.request_text', return_value=html) as fetch:
            items = fetch_eeas_tenders(['photography'], probe=probe)
        self.assertEqual(fetch.call_count, 6)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]['deadline'], '2099-12-31')
        self.assertEqual(probe.rows_parsed, 1)

    def test_eeas_uses_deadline_on_detail_page_and_filters_expired(self):
        listing = '<article><a href="/eeas/tender-photo_en">Tender for photography</a></article>'
        detail = '<main><h1>Tender for photography</h1><p>Deadline to express your interest (inscription): 20 November 2026</p></main>'
        def respond(url, **kwargs):
            return detail if url.endswith('tender-photo_en') else listing
        with patch('api._lib.request_text', side_effect=respond), patch('api._lib.is_open_deadline', return_value=False) as is_open:
            self.assertEqual(fetch_eeas_tenders(['photography']), [])
        is_open.assert_called_once_with('2026-11-20')

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
