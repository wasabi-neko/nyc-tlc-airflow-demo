import boto3.session
import boto3, botocore
import json
import botocore.config
import requests
import logging
from io import BytesIO
from tqdm.contrib.concurrent import thread_map
from string import Template

logging.basicConfig(
    filename="./prepare_data.log",
    level=logging.INFO,
    format='[%(asctime)s][%(levelname)s][%(funcName)s]: %(message)s'
)

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

def upload_url_to_s3(url: str, bucket_name: str, s3_key: str):
    try:
        res = requests.get(url, stream=True)
        res.raise_for_status()

        client = create_s3_client()
        client.upload_fileobj(BytesIO(res.content), bucket_name, s3_key)
        return (url, True)
    except Exception as e:
        logging.error(f"Upload filaed, file:{s3_key}, url:{url}, {e}")
        return (url, False)

def upload_urls_parallel(urls: list[str], bucket_name: str):
    # (url, client, bucket_name, key)
    inputs = [(url, bucket_name, url.split('/')[-1]) for url in urls]
    # Transform fomr "a list of tuple" to "a tuple of list"
    inputs = tuple(map(list, zip(*inputs)))
    # tqdm threadPoolExecutor wrapper
    results = thread_map(upload_url_to_s3, *inputs, max_workers=32,desc="Uploading", unit="file")
    return results


if __name__ == "__main__":

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
    logging.info(f"Finished Uploading; result: {results}")
