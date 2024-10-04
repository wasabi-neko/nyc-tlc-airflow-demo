from enum import Enum
import boto3.session
import boto3, botocore
from botocore.exceptions import ClientError
import json
import botocore.config
import requests
import logging
import argparse
from io import BytesIO
from tqdm.contrib.concurrent import thread_map
from string import Template

parser = argparse.ArgumentParser()
parser.add_argument('--debug', action='store_true', help='log in debug mode.')
parser.add_argument('--overwrite', action='store_true', help='overwrite existing file in S3 bueckt.')
args = parser.parse_args()

logging.basicConfig(
    filename="./prepare_data.log",
    level=logging.INFO,
    format='[%(asctime)s][%(levelname)s][%(funcName)s]: %(message)s'
)

class UploadStatus(Enum):
    SKIPPED = 0
    UPLOADED = 1
    FILAED = 2

def create_s3_client():
    session = boto3.session.Session()

    client_config = botocore.config.Config(
        retries={"max_attempts": 10},
        max_pool_connections=50,
    )
    client = session.client(
        's3',
        config=client_config,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret)
    return client

def check_file_exists(client, bucket_name, key):
    try:
        client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise

def upload_url_to_s3(url: str, bucket_name: str, s3_key: str):
    try:
        # create client for each thread
        client = create_s3_client()

        # check file existing or not
        if args.overwrite or not check_file_exists(client, bucket_name, s3_key):
            res = requests.get(url, stream=True)
            res.raise_for_status()
            client.upload_fileobj(BytesIO(res.content), bucket_name, s3_key)
            return {'url': url, status: UploadStatus.UPLOADED}
        else:
            return {'url': url, 'status': UploadStatus.SKIPPED}
    except Exception as e:
        logging.error(f"Upload filaed, file:{s3_key}, url:{url}, {e}")
        return {'url': url, 'status': UploadStatus.FILAED}

def upload_urls_parallel(urls: list[str], bucket_name: str):
    # (url, client, bucket_name, key)
    inputs = [(url, bucket_name, url.split('/')[-1]) for url in urls]
    # Transform fomr "a list of tuple" to "a tuple of list"
    inputs = tuple(map(list, zip(*inputs)))
    # tqdm threadPoolExecutor wrapper
    results = thread_map(upload_url_to_s3, *inputs, max_workers=32,desc="Uploading", unit="file")
    return results


if __name__ == "__main__":
    print(f"with arg: debug: {args.debug}, overwrite: {args.overwrite}")

    # load crediential
    with open('./aws_secret_config.json') as file:
        config = json.load(file)
        access_key = config.get("ACCESS_KEY")
        secret = config.get("SECRET_KEY")

    # year, month
    year_range = range(2009, 2010)
    month_range = range(1, 7)
    url_template = Template('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-$month.parquet')
    urls = [url_template.substitute(year=str(y), month=f"{m:02d}") for y in year_range for m in month_range]
    bucket_name = "nyc-tlc-demo"

    logging.info("Start Upload file")
    results = upload_urls_parallel(urls, bucket_name)
    logging.info(f"Finished Uploading;")

    logging.debug(f"Results: {results}")


    total_uploads = len(results)
    success_cnt = 0
    skip_cnt = 0
    fail_cnt = 0

    for result in results:
        if result['status'] == UploadStatus.UPLOADED:
            success_cnt += 1
        elif result['status'] == UploadStatus.FILAED:
            fail_cnt += 1
        elif result['status'] == UploadStatus.SKIPPED:
            skip_cnt += 1

    logging.info(f"skip: {skip_cnt}, uploaded: {success_cnt}, failed: {fail_cnt}, totoal: {total_uploads}")


