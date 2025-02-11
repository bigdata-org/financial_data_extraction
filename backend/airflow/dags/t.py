import os 
from dotenv import load_dotenv
import json
from utils.aws import s3
from utils.snowflake.snowflake_connector import conn
from io import BytesIO
import requests
import zipfile
import time 



def test():
    headers = {
        'User-Agent': 'Michigan State University bigdataintelligence@gmail.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Host': 'www.sec.gov',
        'Connection': 'keep-alive',
        'Referer': 'https://www.sec.gov/edgar/searchedgar/companysearch.html'
    }

    year = "2024"
    qtr = "3"
    bucket_name = os.getenv('BUCKET_NAME')
    region = os.getenv('REGION')
    # s3_key = "dumps/metadata.json"
    s3_client = s3.get_s3_client()

    s3_url =f"https://s3linkscrapper.s3.us-east-2.amazonaws.com/dumps/metadata.json"

    data  = requests.get(s3_url)
    json_data = json.loads(data.content)
    zip_link = json_data[year][qtr]
    try :
        time.sleep(0.15)
        zip_response = requests.get(zip_link,stream=True,headers=headers)
        # zip_response.raise_for_status()
        zip_buffer = BytesIO(zip_response.content)
        # if not zipfile.is_zipfile(zipfile):
        #     print("invalid")

        # zip_buffer
        temp_folder =f"sec_data/year={year}/qtr={qtr}/"

        with zipfile.ZipFile(zip_buffer) as zip:
            for file_name in zip.namelist():
                file_data = zip.read(file_name)
                s3_key = f"{temp_folder}{file_name.split('.')[0]}.tsv"
                s3_client.put_object(
                    Bucket = bucket_name,
                    Key = s3_key,
                    Body =file_data
                )
                print(f"uploaded file {file_name}")

    except Exception as e:
        print({"error":str(e)})

# def test_sf(year, qtr):
#     bucket_name  =os.getenv('S3_BUCKET_NAME')
#     files_path =f"dumps/year" 
#     cur = conn.cursor()

#     cur.execute("select current_user")

#     sql_create_tsv_format  =  """
#                                 CREATE FILE FORMAT IF NOT EXISTS sec_tsv_format 
#                                 TYPE = 'CSV' 
#                                 FIELD_DELIMITER = '\t'  -- Tab delimiter for TSV
#                                 SKIP_HEADER = 1         -- Skip the first row (header) if needed
#                                 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                            
#                               """
#     sql_create_external_stage = f"""
#                                 CREATE STAGE IF NOT EXISTS sec_s3_stage
#                                 URL = 's3://{bucket_name}/sec_data/'  
#                                 FILE_FORMAT = sec_tsv_format         
#                                 CREDENTIALS = (
#                                     AWS_KEY_ID = '{os.getenv('ACCESS_KEY')}' 
#                                     AWS_SECRET_KEY = '{os.getenv('SECRET_ACCESS_KEY')}'
#                                 );
#                                 """

#     try:
#         cur.execute(sql_create_tsv_format)
#         print("successfully created format for TSV file")

#         cur.execute(sql_create_external_stage)
#         print("successfully created s3_stage")

#     except Exception as e :
#         print(str(e))

#     cur.close()
#     conn.close()


# from fastapi import FastAPI, HTTPException
# import requests
# import os

# app = FastAPI()

# # Airflow API Configuration
# AIRFLOW_BASE_URL = "http://localhost:8081/api/v1"  # Update if running on a server
# USERNAME = "airflow"  # Default username
# PASSWORD = "airflow"  # Default password
# DAG_ID = "dag_to_scrape_and_upload"  # Replace with your DAG ID

# @app.post("/trigger-dag/{dag_id}")
# def trigger_dag(dag_id: str):
#     """Trigger an Airflow DAG using the REST API."""
#     url = f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns"

#     response = requests.post(
#         url,
#         auth=(USERNAME, PASSWORD),
#         headers={"Content-Type": "application/json"},
#         json={"conf": {}}  # Optional: Add DAG run config params
#     )

#     if response.status_code in [200, 201]:  # 201 = Created, 200 = Success
#         return {"message": f"DAG {dag_id} triggered successfully!", "response": response.json()}
#     else:
#         raise HTTPException(status_code=response.status_code, detail=response.json())



if __name__ == "__main__":
    test(2024,4)
