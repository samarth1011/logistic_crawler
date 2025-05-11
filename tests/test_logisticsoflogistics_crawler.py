import unittest
from unittest.mock import patch, MagicMock
from crawlers.logisticsoflogistics_crawler import LogisticsOfLogisticsCrawler

class TestLogisticsOfLogisticsCrawler(unittest.TestCase):
    def setUp(self):
        self.crawler = LogisticsOfLogisticsCrawler(bucket_name="logistics-crawler-data")

    @patch("crawlers.logisticsoflogistics_crawler.requests.get")
    def test_fetch_success(self, mock_get):
        mock_response = MagicMock(status_code=200, text="<html><article><h2 class='entry-title'><a href='https://example.com'>Article</a></h2></article></html>")
        mock_get.return_value = mock_response

        html = self.crawler.fetch()
        self.assertIn("<article>", html)



if __name__ == "__main__":
    unittest.main()