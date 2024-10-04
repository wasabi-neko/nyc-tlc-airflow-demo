import requests
import concurrent.futures
from tqdm import tqdm
from string import Template


# year, month
yellow_file_template = Template('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-$month.parquet')

url = yellow_file_template.substitute(year='2024', month='01')
print(url)

# Streaming
res = requests.get(url, stream=True)

total_size = int(res.headers.get('content-length', 0))
block_size = 1024

if res.status_code == 200:
    with tqdm(total=total_size, unit="B", unit_scale=True) as progress_bar:
        with open('./yellow.parquet', 'wb') as file:
            for data in res.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
    print('download finished')
else:
    print('download failed')

