from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
from utils import main


load_dotenv()


with DAG(
    dag_id='dag_for_dbt_raw',
    description='dag to call dbt for raw data',
    start_date=datetime(2025,2,7),
    schedule_interval='@daily'
) as dag :

    upload_data_to_s3 = PythonOperator(
        task_id='Upload_data_to_s3',
        python_callable=main.store_data_to_s3,
        params={
            'year': '2024',  # Default value
            'qtr': '4'       # Default value
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
        "steps_override": ["dbt run --select raw.sub --vars \\"{\\"year\\": \\"{{ params.year }}\\", \\"qtr\\": \\"{{ params.qtr }}\\"}"]
    }'
    """,
    params={
        'year': '2024',
        'qtr': '2'
    }
    )


    upload_data_to_s3 >> dbt_raw    

    