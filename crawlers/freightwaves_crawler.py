import requests
from bs4 import BeautifulSoup
import datetime
import json
import boto3
import hashlib
from botocore.config import Config
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import time
# We can also get this AWS credentials in environment variables
AWS_ACCESS_KEY_ID = (
    "AKIARVGDNGHXJJMHJSA2"  # "AKIA47CRZGZ7MJPANDOS"  # "AKIA47CRZGZ7MJPANDOS"
)
AWS_SECRET_ACCESS_KEY = "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"  # "VEGUIPt3E/x+p5kAHQl5ToFeiszIcWI1oBzVlOP8"  # "VEGUIPt3E/x+p5kAHQl5ToFeiszIcWI1oBzVlOP8"

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
s3_client = session.client("s3", config=Config(signature_version="s3v4"), verify=False)


class FreightWavesCrawler:
    def __init__(self, bucket_name):
        self.base_url = "https://www.freightwaves.com"
        self.session = requests.Session()
        self.s3_bucket = bucket_name
        self.s3_prefix = "freightwaves"
        self.headers = {"User-Agent": "Mozilla/5.0"}

    def fetch(self):
        response = self.session.get(self.base_url, headers=self.headers)
        response.raise_for_status()
        return response.text

    def parse(self, html):
        soup = BeautifulSoup(html, "html.parser")
        articles = []

        for article in soup.find_all("article"):
            title_tag = article.find(["h2", "h3"])
            link_tag = title_tag.find("a") if title_tag else None
            image_tag = article.find("img")
            excerpt_tag = article.find("p")
            time_tag = article.find("time")

            title = title_tag.get_text(strip=True) if title_tag else ""
            url = link_tag["href"] if link_tag and link_tag.has_attr("href") else ""
            if url and not url.startswith("http"):
                url = self.base_url + url
            image = image_tag["src"] if image_tag and image_tag.has_attr("src") else ""
            excerpt = excerpt_tag.get_text(strip=True) if excerpt_tag else ""
            published_date = time_tag["datetime"] if time_tag and time_tag.has_attr("datetime") else ""

            articles.append({
                "title": title,
                "url": url,
                "image": image,
                "excerpt": excerpt,
                "published_date": published_date
            })

        # Extract metadata from <meta> tags
        metadata = {}
        for tag in soup.find_all("meta"):
            if tag.has_attr("name"):
                metadata[tag["name"]] = tag.get("content", "")
            elif tag.has_attr("property"):
                metadata[tag["property"]] = tag.get("content", "")

        return {
            "articles": articles,
            "metadata": metadata
        }

        # Metadata from <meta> tags
        metadata = {
            tag.get("name") or tag.get("property"): tag.get("content", "")
            for tag in soup.find_all("meta")
            if tag.has_attr("content")
        }

        return {
            "articles": articles,
            "metadata": metadata
        }

    def upload_to_s3(self, data, html):
        s3 = s3_client
        now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        html_key = f"{self.s3_prefix}/{now}/raw.html"
        json_key = f"{self.s3_prefix}/{now}/parsed.json"

        s3.put_object(Body=html, Bucket=self.s3_bucket, Key=html_key)
        s3.put_object(Body=json.dumps(data, indent=2), Bucket=self.s3_bucket, Key=json_key)

    def get_articles_with_selenium(self):
        options = Options()
        # options.binary_location = "/opt/headless-chromium"
        # options.add_argument("--headless")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        # options.add_argument("--single-process")
        options.add_argument("--disable-gpu")

        options.binary_location = "C:/Users/sg33702/Downloads/chrome-win64/chrome-win64/chrome.exe"

        service = Service(executable_path="C:/Users/sg33702/Downloads/chromedriver-win64/chromedriver-win64/chromedriver.exe")
        driver = webdriver.Chrome(service=service, options=options)

        # driver = webdriver.Chrome("/opt/chromedriver", options=options)

        try:
            driver.get("https://www.freightwaves.com/news/tag/tariffs")
            time.sleep(3)

            # STEP 1: Collect all article URLs first
            article_elements = driver.find_elements(By.TAG_NAME, "article")
            article_urls = []

            for article in article_elements[:10]:  # Limit to first 10 articles
                try:
                    anchor = article.find_element(By.CSS_SELECTOR, "a.fw-block-post-item-blue")
                    url = anchor.get_attribute("href")
                    article_urls.append(url)
                except Exception as e:
                    print("Skipping article due to error:", e)

            articles = []

            # STEP 2: Visit each article separately
            for url in article_urls:
                try:
                    driver.get(url)
                    time.sleep(2)
                    title = driver.find_element(By.TAG_NAME, "h1").text
                    content = driver.find_element(By.ID, "entry-content").text

                    print("Title:", title)
                    articles.append({
                        "title": title,
                        "url": url,
                        "content": content,
                    })
                except Exception as e:
                    print(f"Error parsing article {url}: {e}")

            return articles
        finally:
            driver.quit()

    def run(self):
        html = ''
        # self.fetch()
        # parsed_data = self.parse(html)
        parsed_data = self.get_articles_with_selenium()
        self.upload_to_s3(parsed_data, html)
    
crawler = FreightWavesCrawler("logistics-crawler-data")
crawler.run()
