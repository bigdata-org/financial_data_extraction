
from firecrawl import FirecrawlApp
import os 
from dotenv import load_dotenv
import json
from utils.aws import s3
from io import BytesIO
import zipfile
import requests 
import time
from boto3.s3.transfer import TransferConfig
from botocore.config import Config


load_dotenv()

bucket_name = os.getenv('S3_BUCKET_NAME')

# get all zip link and store them to s3 
def get_links(web_link = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"):
    try:
        s3_client =  s3.get_s3_client()
        bucket_name = os.getenv('S3_BUCKET_NAME')
        app = FirecrawlApp(api_key='fc-0d5722bd706743c0900235dc38d4651e')
        response = app.scrape_url(url=web_link, params={
        'formats': [ 'links' ],
        'onlyMainContent': False,
        'includeTags': [ 'tr', 'a', 'href' ],
        'excludeTags': [ 'headers' ]
        })

        zip_links  = [links for links in response['links'] if links.endswith('.zip')]

        sec_data = {}
        for link in zip_links:
            year_qtr = link.split('/')[-1].split('.zip')[0]
            year,qtr = year_qtr[:4], year_qtr[-1]
            if year not in sec_data:
                sec_data[year] = {}
            sec_data[year][qtr] = link
        json_sec_data = json.dumps(sec_data)
        data = BytesIO(str(json_sec_data).encode("utf-8"))

        s3_key = "dumps/metadata.json"
        s3_client.put_object(
            Bucket = bucket_name,
            Key = s3_key,
            Body = data,
            ContentType = 'application/json'
        )
    except Exception as e:
        return str(e)
    return "Success"
       


# get files from zip folder and upload them to s3
def store_data_to_s3(**context):

    # year = context['params'].get('year','2024')
    # qtr = context['params'].get('qtr','4')

    ti = context['task_instance']
    year = ti.xcom_pull(task_ids='Check_if_data_exists', key='year')
    qtr = ti.xcom_pull(task_ids='Check_if_data_exists', key='qtr')

    

    if year is None:
        year = context['params']['year']
    if qtr is None:
        qtr = context['params']['qtr']

    # year = params.get('year', '2023')  # Default year
    # qtr = params.get('qtr', '3') 

    headers = {
        'User-Agent': 'MIT  bigdata@gmail.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Host': 'www.sec.gov',
        'Connection': 'keep-alive',
        'Referer': 'https://www.sec.gov/edgar/searchedgar/companysearch.html'
    }
  
    bucket_name = os.getenv('S3_BUCKET_NAME')
    region = os.getenv('REGION')
    s3_client = s3.get_s3_client()
    json_link_key = "dumps/metadata.json"

    s3_url =f"https://{bucket_name}.s3.{region}.amazonaws.com/{json_link_key}"

    data  = requests.get(s3_url)
    json_data = json.loads(data.content)
    zip_link = json_data[year][qtr]
    try :
        with requests.get(zip_link,stream=True,headers=headers) as zip_response:      
            zip_buffer = BytesIO()
            for chunk in zip_response.iter_content(chunk_size=8 * 1024 * 1024):
                zip_buffer.write(chunk)
            zip_buffer.seek(0)
            
            temp_folder =f"sec_data/{year}/{qtr}/"

            config = TransferConfig(
                            multipart_threshold=8 * 1024* 1024,
                            max_concurrency=10,
                            multipart_chunksize=64 * 1024* 1024,
                            use_threads=True
                            )

            with zipfile.ZipFile(zip_buffer, 'r') as zip:
                for file_name in zip.namelist():
                   with zip.open(file_name) as file_obj:
                    bytes_data = file_obj.read()
                    cleaned_data = bytes_data.replace(b"\\", b"")  # Remove backslashes
                    s3_key = f"{temp_folder}{file_name.split('.')[0]}.tsv"
                    cleaned_file_obj = BytesIO(cleaned_data)
                    s3_client.upload_fileobj(
                        Fileobj =cleaned_file_obj,
                        Bucket = bucket_name,
                        Key = s3_key,
                        Config = config
                    )
                    print(f"uploaded file {file_name}")
        return "success"

    except Exception as e:
            print({"error":str(e)})


def check_if_file_exists(**context):
    year = context['params'].get('year','2024')
    qtr = context['params'].get('qtr','4')

    ti = context['task_instance']
    ti.xcom_push(key='year', value=year)
    ti.xcom_push(key='qtr', value=qtr)

    s3_client = s3.get_s3_client()
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=f'sec_data/{year}/{qtr}/'
    )
    Keys = [obj['Key'] for obj in response.get('Contents', [])]
    file_count = len(Keys)

    if file_count >= 4:
        return 'dbt_curl_command'
    return 'Upload_data_to_s3'



    
