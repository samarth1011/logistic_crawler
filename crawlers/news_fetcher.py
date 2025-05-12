# crawlers/news_fetcher.py

import requests
import datetime
import boto3
import os
import json
from botocore.config import Config
from utils.send_email_via_ses import send_email
from config.secret import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

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
        self.errors = []

    def log_error(self, error_type, message):
        self.errors.append({
            "timestamp": datetime.datetime.now().isoformat(),
            "source": "NewsFetcher",
            "error_type": error_type,
            "message": message
        })

    def flush_logs(self):
        if self.errors:
            date_str = datetime.datetime.now().strftime("%Y-%m-%d")
            key = f"logs/errors/{date_str}/news_fetcher_errors.json"
            try:
                self.s3.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=json.dumps(self.errors, indent=2),
                    ContentType="application/json"
                )
                print(f"Uploaded error logs to s3://{self.bucket_name}/{key}")
            except Exception as e:
                print("Failed to upload error logs to S3:", str(e))
            
             # Send Email
            try:
                send_email(
                    subject="FreightWaves Crawler Errors",
                    body=json.dumps(self.errors, indent=2),
                    to_email="godaseanil6@gmail.com",
                    from_email="godaseanil6@gmail.com"
                )
            except Exception as e:
                self.log_error(f"Failed to send email notification: ",str(e))

    def fetch_news(self, query="logistics OR supply chain", page_size=10):
        try:
            url = "https://newsapi.org/v2/everything"
            params = {
                "q": query,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": page_size,
                "apiKey": self.api_key,
            }
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.log_error("FetchNewsError", str(e))
            return None

    def upload_to_s3(self, data):
        try:
            date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")
            key = f"news/newsapi/{date_str}/news.json"
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(data, indent=2),
                ContentType="application/json"
            )
            print(f"Uploaded news to s3://{self.bucket_name}/{key}")
        except Exception as e:
            self.log_error("S3UploadError", str(e))

    def run(self):
        print("Fetching latest logistics/supply chain news...")
        try:
            data = self.fetch_news()
            if data:
                self.upload_to_s3(data)
        except Exception as e:
            self.log_error("RunError", str(e))
        finally:
            self.flush_logs()



if __name__ == "__main__":
    fetcher = NewsFetcher(api_key="", bucket_name="logistics-crawler-data")
    fetcher.run()
