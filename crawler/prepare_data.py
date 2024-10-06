from pprint import pp
from typing import Callable, List, Dict
from enum import Enum
import boto3.session
import boto3, botocore
from botocore.exceptions import ClientError
import json
import botocore.config
import pandas as pd
import requests
import logging
import argparse
from io import BytesIO
from tqdm.contrib.concurrent import thread_map

parser = argparse.ArgumentParser()
parser.add_argument('--debug', action='store_true', help='log in debug mode.')
parser.add_argument('--overwrite', action='store_true', help='overwrite existing file in S3 bueckt.')
parser.add_argument('--list', action='store_true', help='list the urls only. do not donwload and uploads')
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

# Create S3 Client for every Thread
#   According to the AWS Boto3 guild, boto session client is NOT thread-safe
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
            return {'url': url, 'status': UploadStatus.UPLOADED}
        else:
            return {'url': url, 'status': UploadStatus.SKIPPED}
    except Exception as e:
        logging.error(f"Upload filaed, file:{s3_key}, url:{url}, {e}")
        return {'url': url, 'status': UploadStatus.FILAED}

def upload_urls_parallel(urls: list[str], bucket_name: str, file_name_parser: Callable[[str], str]) -> List[Dict]:
    # (url, client, bucket_name, key)
    inputs = [(url, bucket_name, file_name_parser(url)) for url in urls]
    # Transform fomr "a list of tuple" to "a tuple of list"
    inputs = tuple(map(list, zip(*inputs)))
    # tqdm threadPoolExecutor wrapper
    results = thread_map(upload_url_to_s3, *inputs, max_workers=32,desc="Uploading", unit="file")
    return results


def tripdata_parser_gen(prefix: str)-> Callable[[str], str]:
    def _parser(url: str)->str:
        return prefix + url.split('/')[-1]
    return _parser

if __name__ == "__main__":
    print(args)

    # year, month
    bucket_name = "nyc-tlc-demo"

    yellow_trip_data_template = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_%Y-%m.parquet"
    yellow_urls = pd.date_range(start='2009-01', end='2024-07', freq='MS').strftime(yellow_trip_data_template)
    yellow_name_parser = tripdata_parser_gen('trip-data/yellow/')

    green_trip_data_template = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_%Y-%m.parquet"
    green_urls = pd.date_range(start='2013-08', end='2013-09', freq='MS').strftime(green_trip_data_template)
    green_name_parser = tripdata_parser_gen('trip-data/green/')

    fhv_tripdata_template = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_%Y-%m.parquet"
    fhv_urls = pd.date_range(start='2015-01', end='2015-02', freq='MS').strftime(fhv_tripdata_template)
    fhv_name_parser = tripdata_parser_gen('trip-data/fhv/')
    # from 2015-01

    fhvhv_tripdata_template = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_%Y-%m.parquet"
    fhvhv_urls = pd.date_range(start='2019-02', end='2019-03', freq='MS').strftime(fhvhv_tripdata_template)
    fhvhv_name_parser = tripdata_parser_gen('trip-data/fhvhv/')
    # from 2019-02

    # list the file only and end the program
    if args.list:
        pp(yellow_urls)
        exit(0)

    # load crediential
    with open('../secrects.json') as file:
        config = json.load(file)
        access_key = config.get("AWS_ACCESS_KEY")
        secret = config.get("AWS_SECRET_KEY")

    # Start uploading
    logging.info("Start Upload file")

    results = upload_urls_parallel(yellow_urls, bucket_name, yellow_name_parser)
    results = upload_urls_parallel(green_urls, bucket_name, green_name_parser)
    results = upload_urls_parallel(fhv_urls, bucket_name, fhv_name_parser)
    results = upload_urls_parallel(fhvhv_urls, bucket_name, fhvhv_name_parser)

    # End upload, log the result
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


