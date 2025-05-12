import boto3
from botocore.config import Config
from config.secret import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# We can also get this AWS credentials in environment variables
AWS_ACCESS_KEY_ID = AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = AWS_SECRET_ACCESS_KEY

# Create a session
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Create separate clients
s3_client = session.client("s3", config=Config(signature_version="s3v4"), verify=False)
ses_client = session.client("ses", region_name="us-north-1")


from_email = "godaseanil@gmail.com"

def send_email(subject, body, to_email, from_email):
    # response = ses_client.send_email(
    #     Source=from_email,
    #     Destination={"ToAddresses": [to_email]},
    #     Message={
    #         "Subject": {"Data": subject},
    #         "Body": {"Text": {"Data": body}},
    #     }
    # )

    # Send SES EMail Logic here
    # response = ses_client.send_email(
    #     Source=from_email,
    #     Destination={
    #         'ToAddresses': [
    #             to_email,
    #         ]
    #     },
    #     Message={
    #         'Subject': {
    #             'Data': subject,
    #             'Charset': 'UTF-8'
    #         },
    #         'Body': {
    #             'Text': {
    #                 'Data': body,
    #                 'Charset': 'UTF-8'
    #             }
    #         }
    #     }
    # )

    return True
