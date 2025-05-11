import boto3
import datetime
import json

class Logger:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.client("s3")
        self.errors = []

    def log_error(self, source, url, error_type, message):
        self.errors.append({
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "source": source,
            "url": url,
            "error_type": error_type,
            "message": message,
        })

    def flush_to_s3(self):
        if self.errors:
            key = f"logs/errors/{datetime.datetime.now().strftime('%Y-%m-%d')}/errors.json"
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(self.errors, indent=2),
                ContentType="application/json"
            )
