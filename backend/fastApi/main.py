from snowflake.snowflake_connector import conn
from fastapi import FastAPI
import pandas as pd
from aws import s3
import os 
import requests
from typing import Optional


cur =   conn.cursor()
app = FastAPI()
bucket_name = os.getenv('S3_BUCKET_NAME')

# Airflow API Configuration
AIRFLOW_BASE_URL = "http://localhost:8081/api/v1"  
USERNAME = os.getenv('AIRFLOW_USERNAME')  
PASSWORD = os.getenv('AIRFLOW_PASSWORD')  
# DAG_ID = "dag_to_scrape_metadata" 

def check_data_availibility(year, qtr):
    s3_client = s3.get_s3_client()
    
    response = s3_client.list_objects(
        Bucket=bucket_name,
        Prefix=f"sec_data/{year}/{qtr}"
    )
    keys = [obj['Key'] for obj in response.get('Contents',[])]
    
    if len(keys) >= 4:
        return True
    return False 


print(USERNAME)

# get queried data
@app.get("/user_query/{query}")
def user_query(query):
   
#    if check_data_availibility(year, qtr) :   
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=columns)
        return {"status": "success", "data": df.to_dict(orient='records')}
# except Exception as e:
        # return {"status": "error", "message": str(e)}


@app.post("/trigger_dag/{dag_id}")
def trigger_dag(dag_id: str):

    url = f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns"
    response = requests.post(
        url,
        auth=(USERNAME, PASSWORD),
        headers={"Content-Type": "application/json"}
        # json={"conf": {year,}}  
    )
    if response.status_code in [200, 201]:  # 201 = Created, 200 = Success
        return {"message": f"DAG {dag_id} triggered successfully!", "response": response.json()}
    else:
        raise Exception 

    