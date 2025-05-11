import unittest
from unittest.mock import patch, MagicMock
from crawlers.g2_crawler import G2Crawler


class TestG2Crawler(unittest.TestCase):
    def setUp(self):
        self.crawler = G2Crawler(bucket_name="logistics-crawler-data")

    @patch("crawlers.g2_crawler.requests.Session.get")
    def test_fetch_success(self, mock_get):
        mock_response = MagicMock(status_code=200, text="<html><title>G2</title></html>")
        mock_get.return_value = mock_response

        html = self.crawler.fetch()
        self.assertIn("G2", html)

    def test_parse_content(self):
        html = """
        <html>
            <head>
                <meta name="description" content="B2B software reviews.">
                <meta property="og:title" content="G2 Platform">
            </head>
            <body>
                <h1>Welcome to G2</h1>
                <img src="logo.png"/>
                <video src="intro.mp4"></video>
            </body>
        </html>
        """
        parsed = self.crawler.parse(html)
        self.assertIn("Welcome to G2", parsed["text"])
        self.assertIn("logo.png", parsed["images"])
        self.assertIn("intro.mp4", parsed["videos"])
        self.assertEqual(parsed["metadata"]["description"], "B2B software reviews.")
        self.assertEqual(parsed["metadata"]["og:title"], "G2 Platform")
        self.assertEqual(parsed["url"], "https://www.g2.com/")

if __name__ == "__main__":
    unittest.main()