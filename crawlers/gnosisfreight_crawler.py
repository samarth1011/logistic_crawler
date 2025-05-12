from crawlers.base_crawler import BaseCrawler
from bs4 import BeautifulSoup
from datetime import datetime
from s3_handler.upload import S3Uploader
import json
import boto3
from botocore.config import Config
import logging
import requests
from utils.send_email_via_ses import send_email
from config.secret import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


class GnosisFreightCrawler:
    def __init__(self, bucket_name):
        self.base_url = "https://www.gnosisfreight.com"
        self.session = requests.Session()
        self.s3_bucket = bucket_name
        self.s3_prefix = "gnosisfreight"
        self.bucket_name = bucket_name
        self.headers = HEADERS
        self.errors = []


    def log_error(self, error_type, message):
        self.errors.append({
            "timestamp": datetime.utcnow().isoformat(),
            "url": self.base_url,
            "error_type": error_type,
            "message": message
        })
        logger.error(f"[{error_type}] {message}")

    def flush_logs(self):
        if self.errors:
            log_key = f"logs/errors/{datetime.utcnow().strftime('%Y-%m-%d')}/gnosisfreight_errors.json"
            log_content = json.dumps(self.errors, indent=2)
            try:
                s3_client.put_object(
                    Bucket="logistics-crawler-data",
                    Key=log_key,
                    Body=log_content,
                    ContentType="application/json"
                )
            except Exception as e:
                logger.exception(f"Failed to upload error logs to S3: {e}")
             # Send Email
            try:
                send_email(
                    subject="GnosisFreightCrawler Errors",
                    body=log_content,
                    to_email="godaseanil6@gmail.com",
                    from_email="godaseanil6@gmail.com"
                )
            except Exception as e:
                logger.exception(f"Failed to send email notification: {e}")

    def fetch(self):
        try:
            response = requests.get(self.base_url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            return response.text
        except Exception as e:
            self.log_error("FetchError", str(e))
            return None

    def parse(self, html):
        try:
            soup = BeautifulSoup(html, "html.parser")

            # Extract text
            text = soup.get_text()

            # Extract metadata
            title = soup.title.string if soup.title else ""
            description_tag = soup.find("meta", attrs={"name": "description"})
            og_title_tag = soup.find("meta", property="og:title")
            og_description_tag = soup.find("meta", property="og:description")

            metadata = {
                "title": title,
                "description": description_tag["content"] if description_tag else "",
                "og:title": og_title_tag["content"] if og_title_tag else "",
                "og:description": og_description_tag["content"] if og_description_tag else "",
                "fetched_at": datetime.utcnow().isoformat()
            }

            # Extract media
            images = [img["src"] for img in soup.find_all("img", src=True)]
            videos = [video["src"] for video in soup.find_all("video", src=True)]

            return {
                "text": text.strip(),
                "metadata": metadata,
                "media": {
                    "images": images,
                    "videos": videos
                }
            }
        except Exception as e:
            self.log_error("ParseError", str(e))
            return None

    def upload_to_s3(self, raw_html, parsed_data):
        try:
            uploader = S3Uploader(self.bucket_name)
            uploader.upload("gnosisfreight", raw_html, parsed_data)
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
    crawler = GnosisFreightCrawler()
    crawler.run()
