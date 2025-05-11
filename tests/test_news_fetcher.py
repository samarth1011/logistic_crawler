import unittest
from unittest.mock import patch, MagicMock
from crawlers.news_fetcher import NewsFetcher


class TestNewsFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = NewsFetcher(api_key="eaa2bd8dbd5946f4ac81cdb1f8f6b1a4", bucket_name="logistics-crawler-data")

    @patch("crawlers.news_fetcher.requests.get")
    def test_fetch_news_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "ok",
            "totalResults": 1,
            "articles": [
                {
                    "title": "Supply Chain Breakthrough",
                    "description": "Big changes in logistics",
                    "url": "https://example.com/news",
                    "publishedAt": "2024-05-10T10:00:00Z"
                }
            ]
        }
        mock_get.return_value = mock_response

        data = self.fetcher.fetch_news()
        self.assertEqual(data["status"], "ok")
        self.assertEqual(len(data["articles"]), 1)
        self.assertEqual(data["articles"][0]["title"], "Supply Chain Breakthrough")

    @patch("crawlers.news_fetcher.s3_client.put_object")
    def test_upload_to_s3(self, mock_put):
        sample_data = {
            "status": "ok",
            "articles": [{"title": "Sample"}]
        }
        self.fetcher.upload_to_s3(sample_data)
        self.assertTrue(mock_put.called)
        args, kwargs = mock_put.call_args
        self.assertEqual(kwargs["Bucket"], "logistics-crawler-data")
        self.assertIn("newsapi", kwargs["Key"])
        self.assertEqual(kwargs["ContentType"], "application/json")


if __name__ == "__main__":
    unittest.main()
