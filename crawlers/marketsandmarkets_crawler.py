import requests
from bs4 import BeautifulSoup
import boto3
import os
import datetime
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


class MarketsAndMarketsCrawler:
    def __init__(self):
        self.url = "https://www.marketsandmarkets.com/Market-Reports/supply-chain-management-market-190997554.html"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0"
        })
        self.bucket_name = os.environ.get("S3_BUCKET", "your-default-bucket-name")

    def run(self):
        html = self.fetch()
        parsed = self.parse(html)
        self.upload_to_s3({
            "url": self.url,
            "raw_html": html,
            "parsed": parsed
        })

    def fetch(self):
        response = self.session.get(self.url)
        response.raise_for_status()
        return response.text

    def parse(self, html):
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator="\n", strip=True)
        metadata = {tag.get("name"): tag.get("content") for tag in soup.find_all("meta") if tag.get("name")}
        title = soup.title.string.strip() if soup.title and soup.title.string else ""
        description = soup.find("meta", {"name": "description"})
        description = description["content"].strip() if description else ""

        images = [img["src"] for img in soup.find_all("img") if img.get("src")]

        return {
            "title": title,
            "description": description,
            "text":text,
            "metadata": metadata,
            "images": images,
            "text": soup.get_text()
        }

    def upload_to_s3(self, data):
        s3 = boto3.client("s3")
        date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        key = f"marketsandmarkets/{date_str}/data.json"
        s3.put_object(Bucket=self.bucket_name, Key=key, Body=json.dumps(data), ContentType="application/json")


# Run it locally to test
if __name__ == "__main__":
    crawler = MarketsAndMarketsCrawler()
    crawler.run()
