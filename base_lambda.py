import os
import requests
import json
import ratelimit
import boto3
from datetime import datetime, timezone
from os import listdir
from os.path import isfile, join

from backoff import on_exception, expo
from schedule import every, repeat, run_pending

initial_url = 'https://api.salic.cultura.gov.br/v1/propostas/?limit=100&format=json'

# response = requests.get(initial_url)
# json_output = response.json()

# total_items = json_output["total"]

# propostas_data = json_output["_embedded"]["propostas"]

# print(json_output["total"])

# print(f"{type(propostas_data)=}\n{type(propostas_data[0])=}\n{type(total_items)=}")

# Configurar o cliente S3
s3 = boto3.client('s3')

# Nome do bucket S3
bucket_name = 'raw-salic-aws-ingestion'

def write_to_s3(data, part):
    file_key = f'propostas_data_part_{part}.json'
    s3.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=json.dumps(data)
    )
    print(f"Dados salvos no S3 em {bucket_name}/{file_key}")

def write_local(data, part):
    folder = 'downloaded'
    if not os.path.exists(folder):
        os.makedirs(folder)
    file_path = join(folder, f'propostas_data_part_{str(part).zfill(3)}.json')
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"Dados salvos localmente em {file_path}")

part = 0
url = initial_url

@on_exception(expo, ratelimit.RateLimitException, max_tries=30)
@ratelimit.limits(calls=50, period=60)
@on_exception(expo, requests.exceptions.HTTPError, max_tries=15)
def get_data(url):
    response = requests.get(url)
    # response.encoding = 'utf-8'
    return response.json()

response = requests.get(url)
json_output = response.json()

while True:
    cache_propostas = []
    for i in range(10):
        json_output = get_data(url)
        print(f"Parte {part}, lida.")
        cache_propostas+= json_output["_embedded"]["propostas"]
        part += 1
        next_url = json_output["_links"]["next"]
        url = next_url

    # write_to_s3(propostas_data, part)
    write_local(cache_propostas, int(part/10))

    if json_output["_links"]["self"] == json_output["_links"]["last"]:
        break

    
    







# def lambda_handler(event, context):
#     N = get_num_records()
#     download_data(N)
#     key = _get_key()
#     files = [f for f in listdir(LOCAL_FILE_SYS) if isfile(join(LOCAL_FILE_SYS, f))]
#     for f in files:
#         s3_client.upload_file(LOCAL_FILE_SYS + "/" + f, S3_BUCKET, key + f)