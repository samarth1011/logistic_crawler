from concurrent.futures import ThreadPoolExecutor
from crawlers.gnosisfreight_crawler import GnosisFreightCrawler
from crawlers.news_fetcher import NewsFetcher
from crawlers.logisticsoflogistics_crawler import LogisticsOfLogisticsCrawler
from crawlers.freightwaves_crawler import FreightWavesCrawler
from crawlers.g2_crawler import G2Crawler
from crawlers.marketsandmarkets_crawler import MarketsAndMarketsCrawler

def lambda_handler(event, context):
    crawlers = [
        GnosisFreightCrawler(bucket_name="logistics-crawler-data"),
        LogisticsOfLogisticsCrawler(bucket_name="logistics-crawler-data"),
        FreightWavesCrawler(bucket_name="logistics-crawler-data"),
        G2Crawler(bucket_name="logistics-crawler-data"),
        MarketsAndMarketsCrawler(bucket_name="logistics-crawler-data"),
        NewsFetcher(api_key="eaa2bd8dbd5946f4ac81cdb1f8f6b1a4", bucket_name="logistics-crawler-data")
    ]

    def run_crawler(crawler):
        try:
            crawler.run()
        except Exception as e:
            print(f"Error running {crawler.__class__.__name__}: {e}")

    with ThreadPoolExecutor(max_workers=6) as executor:
        executor.map(run_crawler, crawlers)

    return {
        "statusCode": 200,
        "body": "Parallel crawl and upload completed"
    }
