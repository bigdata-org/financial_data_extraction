from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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

default_args = {
    'owner' : 'tanmay',
    'retries': 5,
    'retry_delay' : timedelta(minutes=5)
}


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
       

def store_data_to_s3(**context):

    params = context['params']
    year = params.get('year', '2023')  # Default year
    qtr = params.get('qtr', '3') 

    headers = {
        'User-Agent': 'MIT  bigdata@gmail.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Host': 'www.sec.gov',
        'Connection': 'keep-alive',
        'Referer': 'https://www.sec.gov/edgar/searchedgar/companysearch.html'
    }
    # year = "2024"
    # qtr = "4"
    bucket_name = os.getenv('S3_BUCKET_NAME')
    region = os.getenv('REGION')
    s3_client = s3.get_s3_client()
    json_link_key = "dumps/metadata.json"

    s3_url =f"https://{bucket_name}.s3.{region}.amazonaws.com/{json_link_key}"

    data  = requests.get(s3_url)
    json_data = json.loads(data.content)
    zip_link = json_data[year][qtr]
    try :
        time.sleep(0.15)
        with requests.get(zip_link,stream=True,headers=headers) as zip_response:      
            zip_buffer = BytesIO()
            for chunk in zip_response.iter_content(chunk_size=8 * 1024 * 1024):
                zip_buffer.write(chunk)
            zip_buffer.seek(0)
            
            temp_folder =f"sec_data/{year}/qtr{qtr}/"

            config = TransferConfig(
                            multipart_threshold=8 * 1024* 1024,
                            max_concurrency=5,
                            multipart_chunksize=8 * 1024* 1024,
                            use_threads=True
                            )

            with zipfile.ZipFile(zip_buffer, 'r') as zip:
                for file_name in zip.namelist():
                   with zip.open(file_name) as file_obj:
                    s3_key = f"{temp_folder}{file_name.split('.')[0]}.tsv"
                 
                    s3_client.upload_fileobj(
                        Fileobj =file_obj,
                        Bucket = bucket_name,
                        Key = s3_key,
                        Config = config
                    )
                    print(f"uploaded file {file_name}")
        return "success"

    except Exception as e:
            print({"error":str(e)})

# def invoke_dbt(**context):
#      params = context['params']
#      year = params.get('year','2024')
#      qtr = params.get('qtr','1')
#      bash_command = f"""
#     curl -X POST "https://bn544.us1.dbt.com/api/v2/accounts/70471823424708/jobs/70471823425377/run/" \
#     -H "Authorization: Token dbtu_aSKjCT4DBp7NVMX7ZIOj9Meosndn7W8Y2pD3K--X3a-v_pArxA" \
#     -H "Content-Type: application/json" \
#     -d '{{"cause":"Triggered via API","steps_override":["dbt run --select raw.sub --vars \\"{{\\\"year\\\":\\\"{year}\\\", \\\"qtr\\\": \\\"{qtr}\\\"}}\\""]}}'
#     """
#      return bash_command
    


with DAG(
    # default_args= default_args,
    dag_id='dag_to_scrape_and_upload',
    description='dag_with_python',
    start_date=datetime(2025,2,7),
    schedule_interval='@daily'
) as dag :
    
    # scrape_links = PythonOperator(
    #     task_id='fireCrawl',
    #     python_callable=get_links
    # )

    # upload_datas3 = PythonOperator(
    #     task_id='Upload_data_to_s3',
    #     python_callable=store_data_to_s3,
    #     params={
    #         'year': '2024',  # Default value
    #         'qtr': '4'       # Default value
    #     }
    # )

    test_dbt = BashOperator(
    task_id="test_dbt_curl",
    bash_command="""
  curl -X POST "https://bn544.us1.dbt.com/api/v2/accounts/70471823424708/jobs/70471823425377/run/" \
    -H "Authorization: Token dbtu_aSKjCT4DBp7NVMX7ZIOj9Meosndn7W8Y2pD3K--X3a-v_pArxA" \
    -H "Content-Type: application/json" \
    -d '{
        "cause": "Triggered via API",
        "steps_override": ["dbt run --select raw.sub --vars \\"{\\"year\\": \\"{{ params.year }}\\", \\"qtr\\": \\"{{ params.qtr }}\\"}\\""]
    }'
    """,
    params={
        'year': '2024',
        'qtr': '2'
    }
    )
        

    

    # upload_datas3
    test_dbt




    # invoke_dbt
# with DAG(
#     dag_id='dag_to_upload_data_to_s3',
#     description='python script to upload data',
#     start_date=datetime(2025,2,7),
#     schedule_interval='@monthly'

# ) as dag:
#     pass