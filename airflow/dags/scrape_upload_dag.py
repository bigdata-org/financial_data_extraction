from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
from utils.main import store_data_to_s3


load_dotenv()

with DAG(
    # default_args= default_args,
    dag_id='dag_to_scrape_and_upload',
    description='dag_with_python',
    start_date=datetime(2025,2,7),
    schedule_interval='@daily'
) as dag :

    upload_datas3 = PythonOperator(
        task_id='Upload_data_to_s3',
        python_callable=store_data_to_s3,
        params={
            'year': '2024',  # Default value
            'qtr': '4'       # Default value
        }
    )


    upload_datas3
