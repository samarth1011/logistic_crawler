# import unittest
# from unittest.mock import patch, MagicMock
# from crawlers.freightwaves_crawler import FreightWavesCrawler

# class TestFreightWavesCrawler(unittest.TestCase):
#     def setUp(self):
#         self.crawler = FreightWavesCrawler(bucket_name="logistics-crawler-data")

#     @patch("crawlers.freightwaves_crawler.requests.Session.get")
#     def test_fetch_success(self, mock_get):
#         mock_response = MagicMock(status_code=200, text="<html><h1>News Title</h1></html>")
#         mock_get.return_value = mock_response
#         html = self.crawler.fetch()
#         self.assertIn("News Title", html)

#     def test_parse_html(self):
#         html = "<html><head><title>Article Title</title></head><body>Some content here.</body></html>"
#         parsed = self.crawler.parse(html)
#         self.assertIn("Article Title", parsed["title"])
#         self.assertIn("Some content here", parsed["text"])

# if __name__ == "__main__":
#     unittest.main()
