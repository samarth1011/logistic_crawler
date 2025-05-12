import requests
from bs4 import BeautifulSoup
import datetime
import boto3
import hashlib
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


class LogisticsOfLogisticsCrawler:
    def __init__(self, bucket_name):
        self.base_url = "https://www.thelogisticsoflogistics.com/"
        self.bucket_name = bucket_name
        self.s3 = s3_client
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
            key = f"logs/errors/{datetime.datetime.utcnow().strftime('%Y-%m-%d')}/logistics_errors.json"
            self.s3.put_object(
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
                self.log_error(f"Failed to send email notification: ",str(e))

    def fetch(self):
        try:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            }
            response = requests.get(self.base_url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            self.log_error("FetchError", str(e))
            raise

    def parse(self, html):
        try:
            soup = BeautifulSoup(html, "html.parser")
            articles = soup.find_all("article")
            results = []

            for article in articles:
                title_tag = article.find("h2", class_="entry-title")
                title = title_tag.text.strip() if title_tag else ""

                url_tag = title_tag.find("a") if title_tag else None
                url = url_tag["href"] if url_tag and "href" in url_tag.attrs else ""

                excerpt_tag = article.find("div", class_="entry-summary")
                excerpt = excerpt_tag.text.strip() if excerpt_tag else ""

                image_tag = article.find("img")
                image = image_tag["src"] if image_tag and "src" in image_tag.attrs else ""

                published_date = article.find("time")
                pub_date = published_date["datetime"] if published_date and "datetime" in published_date.attrs else ""

                if title or url:
                    results.append({
                        "title": title,
                        "url": url,
                        "published_date": pub_date,
                        "excerpt": excerpt,
                        "image": image,
                    })

            return results
        except Exception as e:
            self.log_error("ParseError", str(e))
            raise

    def upload_to_s3(self, parsed_data, raw_html):
        try:
            date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")
            folder = f"logisticsoflogistics/{date_str}/"

            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=f"{folder}parsed.json",
                Body=json.dumps(parsed_data, indent=2),
                ContentType="application/json"
            )

            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=f"{folder}raw.html",
                Body=raw_html,
                ContentType="text/html"
            )
        except Exception as e:
            self.log_error("S3UploadError", str(e))
            raise

    def run(self):
        try:
            html = self.fetch()
            parsed = self.parse(html)
            self.upload_to_s3(parsed, html)
            print(f"Uploaded LogisticsOfLogistics data to s3://{self.bucket_name}")
        except Exception as e:
            self.log_error("RunError", str(e))
        finally:
            self.flush_logs()


if __name__ == "__main__":
    crawler = LogisticsOfLogisticsCrawler(bucket_name="logistics-crawler-data")
    crawler.run()
