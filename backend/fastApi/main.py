from snowflake_python.snowflake_connector  import conn
from fastapi import FastAPI
import pandas as pd
from aws import s3
import os 
import uvicorn
import requests
from dotenv import load_dotenv


load_dotenv()
app = FastAPI()
bucket_name = os.getenv('S3_BUCKET_NAME')

# Airflow API Configuration
AIRFLOW_BASE_URL = "http://35.209.89.127:8080/api/v1"  
USERNAME = os.getenv('AIRFLOW_USERNAME')  
PASSWORD = os.getenv('AIRFLOW_PASSWORD')  

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


def trigger_dag(dag_id: str, year, qtr):

    url = f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns"   
    response = requests.post(
        url,
        auth=(USERNAME, PASSWORD),
        headers={"Content-Type": "application/json"},
        json={
            "conf": {
                "year": year,
                "qtr": qtr
            }
        }  
    )
    if response.status_code in [200, 201]:  # 201 = Created, 200 = Success
        return {"message": f"DAG {dag_id} triggered successfully!", "response": response.json()}
    else:
        raise Exception 

# get queried data
@app.get("/user_query/{query}/{year}/{qtr}/{schema}")
def user_query(query:str,year:str, qtr:str,schema:str):
    try:
        check_sf = {
            "raw" : f"select nvl(count(adsh),0) data from raw.sub where year(filed)={year} and quarter(filed)={qtr};",
            "json":f"select nvl(count(data_hash),0) as data from json.sub where year(data:FILED::date)={year} and quarter(data:FILED::date)={qtr};",
            "dw" : f"select count(fct_bs_sk) as data from dw.fact_balance_sheet where year(filing_date)={year} and quarter(filing_date)={qtr};"
        }

        cur = conn.cursor()
        sf_response = cur.execute(check_sf[schema.lower()])
        result = sf_response.fetchall()
        df = pd.DataFrame(result)
        data = df.iloc[0, 0]
        
        if data > 0 :
            limit_query = query + " Limit 100" 
            cur.execute(f"USE SCHEMA {conn.database}.{schema}")
            cur.execute(limit_query)
            
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()

            df = pd.DataFrame(data, columns=columns)
            df = df.astype(str)  

            return {"status": "success", "data": df.to_dict(orient='records')}           
        else : 
            taskname = schema.lower()
            task_dict = {
                "raw" : "dag_for_dbt_raw",
                "json":"dag_for_dbt_json",
                "dw" :"dag_for_dbt_denormalize"
            }
            
            resposne = trigger_dag(task_dict[taskname],year,qtr)
            # return f"Run Airflow pipeline for {year} and {qtr}" 
            return resposne

    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        # Close cursor after use
        if 'cur' in locals():
            cur.close()



# if __name__ == "__main__":
#     uvicorn.run("main:app", host="127.0.0.1", port=8082, reload=True)