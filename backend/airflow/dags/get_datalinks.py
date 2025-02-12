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
from utils.main import get_links


load_dotenv()
with DAG(
    # default_args= default_args,
    dag_id='dag_to_scrape_metadata',
    description='scraping zip file links',
    start_date=datetime(2025,2,7),
    schedule_interval='@daily'
) as dag :
    
    scrape_links = PythonOperator(
        task_id='fireCrawl',
        python_callable=get_links
    )
    

    scrape_links


