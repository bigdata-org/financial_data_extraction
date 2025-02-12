from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator 
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
from utils.main import check_if_file_exists
from utils.main import store_data_to_s3



load_dotenv()
with DAG(
    dag_id='dag_for_dbt_json',
    description='dag to call dbt for json data',
    start_date=datetime(2025,2,7),
    schedule_interval='@monthly'
) as dag :
    
    check_data_exists = BranchPythonOperator(
        task_id='Check_if_data_exists',
        python_callable=check_if_file_exists,
        params={
            'year' : '2024',
            'qtr'  : '1'
        }
    )

    upload_data_to_s3 = PythonOperator(
        task_id='Upload_data_to_s3',
        python_callable=store_data_to_s3,
        params={
            'year': '2024',  # Default value
            'qtr' : '4'       # Default value
        }
    )

    dbt_json = BashOperator(
    task_id="dbt_json_command",
    bash_command="""
  

          """,
    params={
        'year': '2024',
        'qtr': '2'
    }
    )

    join = EmptyOperator(
        task_id='join',
        trigger_rule='none_failed'
    )

    check_data_exists >> [upload_data_to_s3, dbt_json]
    upload_data_to_s3 >> join
    dbt_json >> join
    
    

    