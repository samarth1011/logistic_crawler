import requests
from bs4 import BeautifulSoup
import datetime
import boto3
import os
import json
from botocore.config import Config
from utils.send_email_via_ses import send_email
from config.secret import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
import logging
# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# We can also get this AWS credentials in environment variables
AWS_ACCESS_KEY_ID = AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = AWS_SECRET_ACCESS_KEY

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
s3_client = session.client("s3", config=Config(signature_version="s3v4"), verify=False)


class G2Crawler:
    def __init__(self, bucket_name="logistics-crawler-data"):
        self.base_url = "https://www.g2.com/"
        self.s3_bucket = bucket_name
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://www.google.com/",
            "Connection": "keep-alive",
        })
        self.errors = []

    def log_error(self, error_type, message):
        self.errors.append({
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "url": self.base_url,
            "error_type": error_type,
            "message": message
        })

    def flush_logs(self):
        if self.errors:
            log_key = f"logs/errors/{datetime.datetime.utcnow().strftime('%Y-%m-%d')}/g2_errors.json"
            try:
                s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=log_key,
                    Body=json.dumps(self.errors, indent=2),
                    ContentType="application/json"
                )
            except Exception as e:
                print("Failed to upload error logs to S3:", str(e))
             # Send Email
            try:
                send_email(
                    subject="G2Crawler Errors",
                    body=json.dumps(self.errors, indent=2),
                    to_email="godaseanil6@gmail.com",
                    from_email="godaseanil6@gmail.com"
                )
            except Exception as e:
                logger.exception(f"Failed to send email notification: {e}")

    def fetch(self):
        try:
            response = self.session.get(self.base_url)
            response.raise_for_status()
            return response.text
        except Exception as e:
            self.log_error("FetchError", str(e))
            return None

    def parse(self, html):
        try:
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text(separator="\n", strip=True)
            images = [img['src'] for img in soup.find_all("img", src=True)]
            videos = [video['src'] for video in soup.find_all("video", src=True)]
            metadata = {
                tag.get("name") or tag.get("property"): tag.get("content")
                for tag in soup.find_all("meta") if tag.get("content")
            }
            return {
                "text": text[:5000],
                "images": images,
                "videos": videos,
                "metadata": metadata,
                "url": self.base_url,
            }
        except Exception as e:
            self.log_error("ParseError", str(e))
            return None

    def upload_to_s3(self, raw_html, parsed_data):
        try:
            date_str = datetime.datetime.now().strftime("%Y-%m-%d")
            s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=f"g2/raw/{date_str}/page.html",
                Body=raw_html,
                ContentType="text/html"
            )
            s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=f"g2/parsed/{date_str}/parsed.json",
                Body=json.dumps(parsed_data, indent=2),
                ContentType="application/json"
            )
        except Exception as e:
            self.log_error("S3UploadError", str(e))

    def run(self):
        html = self.fetch()
        if html:
            parsed = self.parse(html)
            if parsed:
                self.upload_to_s3(html, parsed)
        self.flush_logs()


if __name__ == "__main__":
    crawler = G2Crawler()
    crawler.run()
