import random
import time
import requests
from bs4 import BeautifulSoup

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...",
    # Add more user agents
]

HEADERS = {
    "User-Agent": random.choice(USER_AGENTS)
}

class BaseCrawler:
    def __init__(self, url):
        self.url = url
        self.headers = HEADERS

    def fetch(self):
        try:
            response = requests.get(self.url, headers=self.headers, timeout=10, verify = False)
            if response.status_code == 200:
                return response.text
            else:
                print(f"Failed to fetch {self.url}, status: {response.status_code}")
        except Exception as e:
            print(f"Error fetching {self.url}: {e}")
        return None

    def parse(self, html):
        raise NotImplementedError("Subclasses must implement this method")
