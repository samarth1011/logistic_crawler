import requests
from bs4 import BeautifulSoup
import datetime
import boto3
import os
import json
from botocore.config import Config

from config.secret import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# We can also get this AWS credentials in environment variables
AWS_ACCESS_KEY_ID = AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = AWS_SECRET_ACCESS_KEY

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
s3_client = session.client("s3", config=Config(signature_version="s3v4"), verify=False)


class G2Crawler:
    def __init__(self, bucket_name):
        self.base_url = "https://www.g2.com/"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        })
        self.s3_bucket = bucket_name

    def fetch(self):
        response = self.session.get(self.base_url)
        response.raise_for_status()
        return response.text

    def parse(self, html):
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator="\n", strip=True)
        images = [img['src'] for img in soup.find_all("img", src=True)]
        videos = [video['src'] for video in soup.find_all("video", src=True)]
        metadata = {
            tag.get("name") or tag.get("property"): tag.get("content")
            for tag in soup.find_all("meta") if tag.get("content")
        }
        return {
            "text": text[:5000],  # Limit to avoid large payload
            "images": images,
            "videos": videos,
            "metadata": metadata,
            "url": self.base_url,
        }

    def upload_to_s3(self, raw_html, parsed_data):
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        s3 = s3_client
        s3.put_object(
            Bucket=self.s3_bucket,
            Key=f"g2/raw/{date_str}/page.html",
            Body=raw_html,
            ContentType="text/html"
        )
        s3.put_object(
            Bucket=self.s3_bucket,
            Key=f"g2/parsed/{date_str}/parsed.json",
            Body=json.dumps(parsed_data, indent=2),
            ContentType="application/json"
        )

    def run(self):
        html = self.fetch()
        parsed = self.parse(html)
        self.upload_to_s3(html, parsed)

if __name__ == "__main__":
    crawler = G2Crawler()
    crawler.run()
