import requests
from bs4 import BeautifulSoup
import datetime
import boto3
import hashlib
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


class LogisticsOfLogisticsCrawler:
    def __init__(self, bucket_name):
        self.base_url = "https://www.thelogisticsoflogistics.com/"
        self.bucket_name = bucket_name
        self.s3 = s3_client

    def fetch(self):
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(self.base_url, headers=headers)
        response.raise_for_status()
        return response.text

    def parse(self, html):
        soup = BeautifulSoup(html, "html.parser")
        articles = soup.find_all("article")

        data = []
        for article in articles:
            title_tag = article.find("h2", class_="entry-title")
            title = title_tag.text.strip() if title_tag else ""
            url = title_tag.find("a")["href"] if title_tag and title_tag.find("a") else ""
            date_tag = article.find("time", class_="entry-date")
            pub_date = date_tag["datetime"] if date_tag and date_tag.has_attr("datetime") else ""
            summary = article.find("div", class_="entry-summary")
            excerpt = summary.text.strip() if summary else ""
            image = article.find("img")["src"] if article.find("img") else ""

            data.append({
                "title": title,
                "url": url,
                "published_date": pub_date,
                "excerpt": excerpt,
                "image": image,
            })

        return data

    def upload_to_s3(self, parsed_data, raw_html):
        date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        folder = f"logisticsoflogistics/{date_str}/"

        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=f"{folder}parsed.json",
            Body=json.dumps(parsed_data),
            ContentType="application/json"
        )

        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=f"{folder}raw.html",
            Body=raw_html,
            ContentType="text/html"
        )

    def run(self):
        html = self.fetch()
        parsed = self.parse(html)
        self.upload_to_s3(parsed, html)
        print(f"Uploaded LogisticsOfLogistics data to s3://{self.bucket_name}")
