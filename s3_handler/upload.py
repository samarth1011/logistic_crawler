import boto3
import json
from datetime import datetime
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

class S3Uploader:
    def __init__(self, bucket_name):
        self.s3 = s3_client
        self.bucket = bucket_name

    def upload(self, source, raw_html, parsed_data):
        date_str = datetime.now().strftime("%Y-%m-%d")

        base_key = f"{source}/{date_str}/"
        raw_key = base_key + "raw.html"
        processed_key = base_key + "processed.json"

        # Upload raw HTML
        self.s3.put_object(
            Bucket=self.bucket,
            Key=raw_key,
            Body=raw_html.encode("utf-8"),
            ContentType="text/html"
        )

        # Upload processed JSON
        self.s3.put_object(
            Bucket=self.bucket,
            Key=processed_key,
            Body=json.dumps(parsed_data).encode("utf-8"),
            ContentType="application/json"
        )

        print(f"Uploaded to S3: {raw_key} and {processed_key}")
