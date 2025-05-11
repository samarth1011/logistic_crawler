import unittest
from unittest.mock import patch, MagicMock
from crawlers.marketsandmarkets_crawler import MarketsAndMarketsCrawler


class TestMarketsAndMarketsCrawler(unittest.TestCase):
    def setUp(self):
        self.crawler = MarketsAndMarketsCrawler()
            # bucket_name="logistics-crawler-data"
            

    @patch("crawlers.marketsandmarkets_crawler.requests.Session.get")
    def test_fetch_success(self, mock_get):
        mock_response = MagicMock(status_code=200, text="<html><title>Supply Chain</title></html>")
        mock_get.return_value = mock_response
        html = self.crawler.fetch()
        self.assertIn("Supply Chain", html)


if __name__ == "__main__":
    unittest.main()