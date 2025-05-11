from crawlers.base_crawler import BaseCrawler
from bs4 import BeautifulSoup
import os
from datetime import datetime
from s3_handler.upload import S3Uploader

class GnosisFreightCrawler(BaseCrawler):
    def __init__(self):
        super().__init__("https://www.gnosisfreight.com")

    def parse(self, html):
        soup = BeautifulSoup(html, "html.parser")

        # Text
        text = soup.get_text()

        # Extract metadata properly
        title = soup.title.string if soup.title else ""
        description_tag = soup.find("meta", attrs={"name": "description"})
        og_title_tag = soup.find("meta", property="og:title")
        og_description_tag = soup.find("meta", property="og:description")

        metadata = {
            "title": title,
            "description": description_tag["content"] if description_tag else "",
            "og:title": og_title_tag["content"] if og_title_tag else "",
            "og:description": og_description_tag["content"] if og_description_tag else "",
            "fetched_at": datetime.now().isoformat()
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


    def save_to_local(self, data):
        date_str = datetime.now().strftime("%Y-%m-%d")
        os.makedirs(f"data/gnosisfreight/{date_str}", exist_ok=True)
        with open(f"data/gnosisfreight/{date_str}/raw.html", "w", encoding="utf-8") as f:
            f.write(data["raw_html"])
        with open(f"data/gnosisfreight/{date_str}/processed.txt", "w", encoding="utf-8") as f:
            f.write(data["parsed"]["text"])

    def run(self):
        html = self.fetch()
        if html:
            parsed = self.parse(html)
            # self.save_to_local({"raw_html": html, "parsed": parsed})
            uploader = S3Uploader(bucket_name="logistics-crawler-data") 
            uploader.upload("gnosisfreight", html, parsed)
