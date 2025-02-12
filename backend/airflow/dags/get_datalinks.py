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
from utils import main


load_dotenv()
with DAG(
    # default_args= default_args,
    dag_id='dag_to_scrape_and_upload',
    description='dag_with_python',
    start_date=datetime(2025,2,7),
    schedule_interval='@daily'
) as dag :
    
    scrape_links = PythonOperator(
        task_id='fireCrawl',
        python_callable=main.get_links
    )
    

    scrape_links


