import boto3
import json
import requests
import concurrent.futures
from io import BytesIO
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map, process_map
from string import Template

def upload_url_to_s3(url: str, client, bucket_name: str, s3_key: str):
    try:
        res = requests.get(url, stream=True)
        res.raise_for_status()
        client.upload_fileobj(BytesIO(res.content), bucket_name, s3_key)
        return (url, True)
    except Exception as e:
        print(f"failed upload {url}, Error: {e}")
        return (url, False)

def upload_urls_parallel(urls: list[str], client, bucket_name: str):
    # (url, client, bucket_name, key)
    inputs = [(url, client, bucket_name, url.split('/')[-1]) for url in urls]
    # Transform fomr "a list of tuple" to "a tuple of list"
    inputs = tuple(map(list, zip(*inputs)))
    # tqdm threadPoolExecutor wrapper
    results = thread_map(upload_url_to_s3, *inputs, desc="Uploading", unit="file")
    return results


if __name__ == "__main__":

    # load crediential
    with open('./aws_secret_config.json') as file:
        config = json.load(file)
        access_key = config.get("ACCESS_KEY")
        secret = config.get("SECRET_KEY")

    # client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret)

    # year, month
    year_range = range(2009, 2010)
    month_range = range(1, 13)
    url_template = Template('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-$month.parquet')
    urls = [url_template.substitute(year=str(y), month=f"{m:02d}") for y in year_range for m in month_range]
    bucket_name = "nyc-tlc-demo"

    results = upload_urls_parallel(urls, s3_client, bucket_name)
    print(results)
