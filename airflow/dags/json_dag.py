from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator 
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
from utils.main import store_data_to_s3, check_if_file_exists



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

    dbt_raw = BashOperator(
        task_id="dbt_curl_command",
        bash_command="""
    curl -X POST "https://bn544.us1.dbt.com/api/v2/accounts/70471823424708/jobs/70471823425377/run/" \
        -H "Authorization: Token dbtu_aSKjCT4DBp7NVMX7ZIOj9Meosndn7W8Y2pD3K--X3a-v_pArxA" \
        -H "Content-Type: application/json" \
        -d '{
            "cause": "Triggered via API",
            "steps_override": [
            "dbt run --select raw --vars \\"{\\"year\\": \\"{{ params.year }}\\", \\"qtr\\": \\"{{ params.qtr }}\\"}\\""
            ]
        }'
        """,
       params={
        'year': '{{ task_instance.xcom_pull(key="year") }}',
        'qtr': '{{ task_instance.xcom_pull(key="qtr") }}'
        },
        trigger_rule = 'none_failed_min_one_success'
    )

    dbt_json = BashOperator(
    task_id="dbt_json_command",
    bash_command="""
    curl -X POST "https://bn544.us1.dbt.com/api/v2/accounts/70471823424708/jobs/70471823425377/run/" \
        -H "Authorization: Token dbtu_aSKjCT4DBp7NVMX7ZIOj9Meosndn7W8Y2pD3K--X3a-v_pArxA" \
        -H "Content-Type: application/json" \
        -d '{
            "cause": "Triggered via API",
            "steps_override": [
            "dbt run --select json"
            ]
        }'
        """,
       trigger_rule = 'none_failed_min_one_success'
    )

    
    check_data_exists >> [upload_data_to_s3, dbt_raw]
    upload_data_to_s3  >> dbt_raw
    dbt_raw >> dbt_json

    