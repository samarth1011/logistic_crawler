# crawlers/news_fetcher.py

import requests
import datetime
import boto3
import os
import json
from botocore.config import Config


# Assuming you have set the AWS credentials in your environment variables
AWS_ACCESS_KEY_ID = (
    "AKIARVGDNGHXJJMHJSA2"  # "AKIA47CRZGZ7MJPANDOS"  # "AKIA47CRZGZ7MJPANDOS"
)
AWS_SECRET_ACCESS_KEY = "mx8R82jNTvj0j+CwzAMDW7rORJ5jTkJNDZjFlMZE"  # "VEGUIPt3E/x+p5kAHQl5ToFeiszIcWI1oBzVlOP8"  # "VEGUIPt3E/x+p5kAHQl5ToFeiszIcWI1oBzVlOP8"

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

s3_client = session.client("s3", config=Config(signature_version="s3v4"), verify=False)

class NewsFetcher:
    def __init__(self, api_key, bucket_name):
        self.api_key = api_key
        self.bucket_name = bucket_name
        self.s3 = s3_client
    
    def fetch_news(self, query="logistics OR supply chain", page_size=10):
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": query,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": page_size,
            "apiKey": self.api_key,
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def upload_to_s3(self, data):
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        key = f"news/newsapi/{date_str}/news.json"
        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=json.dumps(data),
            ContentType="application/json"
        )
        print(f"Uploaded news to s3://{self.bucket_name}/{key}")

    def run(self):
        print("Fetching latest logistics/supply chain news...")
        data = self.fetch_news()
        self.upload_to_s3(data)


