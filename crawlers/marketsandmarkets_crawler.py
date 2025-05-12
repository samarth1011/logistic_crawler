import requests
from bs4 import BeautifulSoup
import boto3
import os
import datetime
import json
from botocore.config import Config
from utils.send_email_via_ses import send_email

from config.secret import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
s3_client = session.client("s3", config=Config(signature_version="s3v4"), verify=False)


class MarketsAndMarketsCrawler:
    def __init__(self, bucket_name):
        self.url = "https://www.marketsandmarkets.com/Market-Reports/supply-chain-management-market-190997554.html"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Connection": "keep-alive"
        })
        self.bucket_name = bucket_name
        self.errors = []

    def log_error(self, error_type, message):
        self.errors.append({
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "url": self.url,
            "error_type": error_type,
            "message": message
        })

    def flush_logs(self):
        if self.errors:
            key = f"logs/errors/{datetime.datetime.now().strftime('%Y-%m-%d')}/marketsandmarkets_errors.json"
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(self.errors, indent=2),
                ContentType="application/json"
            )

             # Send Email
            try:
                send_email(
                    subject="FreightWaves Crawler Errors",
                    body=json.dumps(self.errors, indent=2),
                    to_email="godaseanil6@gmail.com",
                    from_email="godaseanil6@gmail.com"
                )
            except Exception as e:
                self.log_error(f"Failed to send email notification:",str(e))

    def run(self):
        try:
            html = self.fetch()
            parsed = self.parse(html)
            self.upload_to_s3(html, parsed)
        except Exception as e:
            self.log_error("RunError", str(e))
        finally:
            self.flush_logs()

    def fetch(self):
        try:
            response = self.session.get(self.url, timeout=50)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            self.log_error("FetchError", str(e))
            raise

    def parse(self, html):
        try:
            soup = BeautifulSoup(html, "html.parser")
            metadata = {tag.get("name"): tag.get("content") for tag in soup.find_all("meta") if tag.get("name")}
            title = soup.title.string.strip() if soup.title else ""
            description = soup.find("meta", {"name": "description"})
            description = description["content"].strip() if description else ""

            images = [img["src"] for img in soup.find_all("img") if img.get("src")]

            return {
                "title": title,
                "description": description,
                "metadata": metadata,
                "images": images,
                "text": soup.get_text()
            }
        except Exception as e:
            self.log_error("ParseError", str(e))
            raise

    def upload_to_s3(self, raw_html, parsed_data):
        try:
            date_str = datetime.datetime.now().strftime("%Y-%m-%d")
            base_path = f"marketsandmarkets/{date_str}/"

            # Upload raw HTML
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=base_path + "raw.html",
                Body=raw_html,
                ContentType="text/html"
            )

            # Upload parsed JSON
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=base_path + "parsed.json",
                Body=json.dumps(parsed_data, indent=2),
                ContentType="application/json"
            )
        except Exception as e:
            self.log_error("S3UploadError", str(e))
            raise


if __name__ == "__main__":
    crawler = MarketsAndMarketsCrawler(bucket_name="logistics-crawler-data")
    crawler.run()
