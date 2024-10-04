import boto3
import json


# load crediential
with open('./aws_secret_config.json') as file:
    config = json.load(file)
    access_key = config.get("ACCESS_KEY")
    secret = config.get("SECRET_KEY")

s3_client = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret)

res = s3_client.list_buckets()
print(res['Buckets'])