from crawlers.gnosisfreight_crawler import GnosisFreightCrawler

def lambda_handler(event, context):
    crawler = GnosisFreightCrawler()
    crawler.run()
    return {
        "statusCode": 200,
        "body": "Crawl and upload completed"
    }
