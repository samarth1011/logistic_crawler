# 🚚 Logistics & Supply Chain Web Crawlers (AWS Lambda-based)

This project is a set of scalable, production-ready Python crawlers designed to fetch, parse, and store data from logistics and supply chain industry websites like:

- [FreightWaves](https://www.freightwaves.com)
- [Markets and Markets](https://www.marketsandmarkets.com)
- [Logistics of Logistics](https://www.logisticsoflogistics.com)
- [G2](https://www.g2.com)
- [Gnosis Freight](https://www.gnosisfreight.com)

All crawlers run in **parallel** via **AWS Lambda** using a `ThreadPoolExecutor`, and data is uploaded to **Amazon S3**. Error handling includes logging and **email notifications via SES**.

---


## 🚀 Features

- ✅ Website scraping using `requests`, `BeautifulSoup`, and `Selenium`
- ✅ HTML + metadata + images + full text extraction
- ✅ Uploads raw + parsed content to Amazon S3
- ✅ Logs errors and sends alerts via AWS SES
- ✅ Multi-threaded for faster parallel crawling
- ✅ Lambda-compatible (including headless Chrome for Selenium)

---


## 🧰 Technologies Used

- Python 3.10+
- [Selenium](https://www.selenium.dev/)
- [BeautifulSoup (bs4)](https://pypi.org/project/beautifulsoup4/)
- [Boto3 (AWS SDK)](https://boto3.amazonaws.com/)
- AWS Lambda, S3, SES
- Chrome Headless + Chromedriver (for Selenium crawlers)

---

```bash
git clone https://github.com/samarth1011/logistic_crawler
cd logistics-crawler

python3 -m venv env
source env/bin/activate
pip install -r requirements.txt

To run Selenium locally, install Chrome & Chromedriver compatible versions.

```

Run Crawlers Locally

```bash
python crawlers/freightwaves_crawler.py

