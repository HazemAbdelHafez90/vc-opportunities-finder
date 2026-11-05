import unittest
import sys
import types

bs4_stub = types.ModuleType("bs4")
bs4_stub.BeautifulSoup = object
sys.modules.setdefault("bs4", bs4_stub)
from api._lib import extract_ungm_verification_headers

TOKEN = "Gx1x0qDwh0GF6Nissy3GYRdor29HHZVrw1Mqq4E1aDDyWKVUMQ0_lCTNtN2bdW7BQNnsT80KZU5ZNoSYjgWPkiKiEgQ1"
PAGE = f'<form><input name="__RequestVerificationToken" type="hidden" value="{TOKEN}" /></form>'
COOKIE = f"__RequestVerificationToken={TOKEN}x; path=/; HttpOnly"


class UngmVerificationHeaderTests(unittest.TestCase):
    def test_the_search_request_carries_the_token_the_cookie_and_the_xhr_marker(self):
        headers = extract_ungm_verification_headers(PAGE, [COOKIE])

        self.assertEqual(headers["RequestVerificationToken"], TOKEN)
        self.assertEqual(headers["Cookie"], f"__RequestVerificationToken={TOKEN}x")
        self.assertEqual(headers["X-Requested-With"], "XMLHttpRequest")

    def test_the_first_token_on_a_page_with_several_forms_is_used(self):
        page = (
            '<input name="__RequestVerificationToken" type="hidden" value="first" />'
            '<input name="__RequestVerificationToken" type="hidden" value="second" />'
        )

        headers = extract_ungm_verification_headers(page, [COOKIE])

        self.assertEqual(headers["RequestVerificationToken"], "first")

    def test_attribute_order_does_not_matter(self):
        page = '<input value="reversed" name="__RequestVerificationToken" type="hidden" />'

        headers = extract_ungm_verification_headers(page, [COOKIE])

        self.assertEqual(headers["RequestVerificationToken"], "reversed")

    def test_unrelated_cookies_are_not_forwarded(self):
        headers = extract_ungm_verification_headers(
            PAGE, ["ASP.NET_SessionId=abc; path=/", COOKIE, "_ga=GA1.2.3; path=/"]
        )

        self.assertEqual(headers["Cookie"], f"__RequestVerificationToken={TOKEN}x")

    def test_a_page_without_a_token_fails_loudly(self):
        with self.assertRaises(RuntimeError):
            extract_ungm_verification_headers("<form></form>", [COOKIE])

    def test_a_response_without_the_cookie_fails_loudly(self):
        with self.assertRaises(RuntimeError):
            extract_ungm_verification_headers(PAGE, ["ASP.NET_SessionId=abc; path=/"])


if __name__ == "__main__":
    unittest.main()
