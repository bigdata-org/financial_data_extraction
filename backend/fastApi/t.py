import os 
from dotenv import load_dotenv
# from utils.aws import s3
from io import BytesIO
import snowflake.connector


load_dotenv()

bucket_name = os

async def test_connection():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SF_ACCOUNT'),
            warehouse="DBT_WH",
            database="SEC",
            schema="RAW"
        )
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_USER()")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return {"status": "success", "user": result[0]}
    except Exception as e:
        return {"status": "error", "message": str(e)}



if __name__ == "__main__":
    test_connection()


# # print(response.get('Contents', []))

# # def get_links(web_link = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"):
# #     try:
# #         s3_client =  s3.get_s3_client()
# #         bucket_name = os.getenv('S3_BUCKET_NAME')
# #         app = FirecrawlApp(api_key='fc-0d5722bd706743c0900235dc38d4651e')
# #         response = app.scrape_url(url=web_link, params={
# #         'formats': [ 'links' ],
# #         'onlyMainContent': False,
# #         'includeTags': [ 'tr', 'a', 'href' ],
# #         'excludeTags': [ 'headers' ]
# #         })

# #         zip_links  = [links for links in response['links'] if links.endswith('.zip')]

# #         sec_data = []
# #         for link in zip_links:
# #             year_qtr = link.split('/')[-1].split('.zip')[0]
# #             year,qtr = year_qtr[:4], year_qtr[-1]
# #             # if year not in sec_data:
# #             sec_data.append({"year":year,"qtr":qtr,"link":link})
# #             # print(sec_data)
# #         json_sec_data = json.dumps(sec_data)
# #         # print(json_sec_data)
# #         data = BytesIO(str(json_sec_data).encode("utf-8"))

# #         s3_key = "dumps/metadata.json"
# #         s3_client.put_object(
# #             Bucket = bucket_name,
# #             Key = s3_key,
# #             Body = data,
# #             ContentType = 'application/json'
# #         )
# #     except Exception as e:
# #         return str(e)
# #     return "Success"

# # def test():
# #     headers = {
# #         'User-Agent': 'Michigan State University bigdataintelligence@gmail.com',
# #         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
# #         'Accept-Encoding': 'gzip, deflate, br',
# #         'Accept-Language': 'en-US,en;q=0.9',
# #         'Host': 'www.sec.gov',
# #         'Connection': 'keep-alive',
# #         'Referer': 'https://www.sec.gov/edgar/searchedgar/companysearch.html'
# #     }

# #     bucket_name = os.getenv('S3_BUCKET_NAME')
# #     region = os.getenv('REGION')
# #     # s3_key = "dumps/metadata.json"
# #     s3_client = s3.get_s3_client()

# #     s3_url =f"https://s3linkscrapper.s3.us-east-2.amazonaws.com/dumps/metadata.json"

# #     data  = requests.get(s3_url)
# #     json_data = json.loads(data.content)
# #     # zip_link = [item for item in json_data]
# #     # zip_link = json_data[year][qtr]
# #     # print(zip_link)

# #     # for item in json_data:
# #     #     print(item['link'])
# #     #     print(item['year'])

# #     for item in  json_data: 
# #         zip_link = item["link"]
# #         year = item["year"]
# #         qtr = item["qtr"]

# #         print(zip_link)
# #         try :
# #             time.sleep(0.15)
# #             with requests.get(zip_link,stream=True,headers=headers) as zip_response:      
# #                 zip_buffer = BytesIO()
# #                 for chunk in zip_response.iter_content(chunk_size=8 * 1024 * 1024):
# #                     zip_buffer.write(chunk)
# #                 zip_buffer.seek(0)
                
# #                 temp_folder =f"sec_data/{year}/{qtr}/"
# #                 config = TransferConfig(
# #                                 multipart_threshold=8 * 1024* 1024,
# #                                 max_concurrency=5,
# #                                 multipart_chunksize=8 * 1024* 1024,
# #                                 use_threads=True
# #                                 )

# #                 with zipfile.ZipFile(zip_buffer, 'r') as zip:
# #                  for file_name in zip.namelist():
# #                     with zip.open(file_name) as file_obj:
# #                         s3_key = f"{temp_folder}{file_name.split('.')[0]}.tsv"
                        
# #                         s3_client.upload_fileobj(
# #                             Fileobj =file_obj,
# #                             Bucket = bucket_name,
# #                             Key = s3_key,
# #                             Config = config
# #                         )
# #                         print(f"uploaded file {file_name}")
# #             #


# #         except Exception as e:
# #                 print({"error":str(e)})
# #     return "success"
    

# # def test_sf(year, qtr):
# #     bucket_name  =os.getenv('S3_BUCKET_NAME')
# #     files_path =f"dumps/year" 
# #     cur = conn.cursor()

# #     cur.execute("select current_user")

# #     sql_create_tsv_format  =  """
# #                                 CREATE FILE FORMAT IF NOT EXISTS sec_tsv_format 
# #                                 TYPE = 'CSV' 
# #                                 FIELD_DELIMITER = '\t'  -- Tab delimiter for TSV
# #                                 SKIP_HEADER = 1         -- Skip the first row (header) if needed
# #                                 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                            
# #                               """
# #     sql_create_external_stage = f"""
# #                                 CREATE STAGE IF NOT EXISTS sec_s3_stage
# #                                 URL = 's3://{bucket_name}/sec_data/'  
# #                                 FILE_FORMAT = sec_tsv_format         
# #                                 CREDENTIALS = (
# #                                     AWS_KEY_ID = '{os.getenv('ACCESS_KEY')}' 
# #                                     AWS_SECRET_KEY = '{os.getenv('SECRET_ACCESS_KEY')}'
# #                                 );
# #                                 """

# #     try:
# #         cur.execute(sql_create_tsv_format)
# #         print("successfully created format for TSV file")

# #         cur.execute(sql_create_external_stage)
# #         print("successfully created s3_stage")

# #     except Exception as e :
# #         print(str(e))

# #     cur.close()
# #     conn.close()


# # from fastapi import FastAPI, HTTPException
# # import requests
# # import os

# # app = FastAPI()

# # # Airflow API Configuration
# # AIRFLOW_BASE_URL = "http://localhost:8081/api/v1"  # Update if running on a server
# # USERNAME = "airflow"  # Default username
# # PASSWORD = "airflow"  # Default password
# # DAG_ID = "dag_to_scrape_and_upload"  # Replace with your DAG ID

# # @app.post("/trigger-dag/{dag_id}")
# # def trigger_dag(dag_id: str):
# #     """Trigger an Airflow DAG using the REST API."""
# #     url = f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns"

# #     response = requests.post(
# #         url,
# #         auth=(USERNAME, PASSWORD),
# #         headers={"Content-Type": "application/json"},
# #         json={"conf": {}}  # Optional: Add DAG run config params
# #     )

# #     if response.status_code in [200, 201]:  # 201 = Created, 200 = Success
# #         return {"message": f"DAG {dag_id} triggered successfully!", "response": response.json()}
# #     else:
# #         raise HTTPException(status_code=response.status_code, detail=response.json())





# # def test2():
# #     headers = {
# #         'User-Agent': 'Michigan State University bigdataintelligence@gmail.com',
# #         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
# #         'Accept-Encoding': 'gzip, deflate, br',
# #         'Accept-Language': 'en-US,en;q=0.9',
# #         'Host': 'www.sec.gov',
# #         'Connection': 'keep-alive',
# #         'Referer': 'https://www.sec.gov/edgar/searchedgar/companysearch.html'
# #     }

# #     bucket_name = os.getenv('S3_BUCKET_NAME')
# #     region = os.getenv('REGION')
# #     # s3_key = "dumps/metadata.json"
# #     s3_client = s3.get_s3_client()

# #     s3_url =f"https://s3linkscrapper.s3.us-east-2.amazonaws.com/dumps/metadata.json"

# #     year = "2022"
# #     qtr = "1"
# #     data  = requests.get(s3_url)
# #     json_data = json.loads(data.content)
# #     zip_link = [item for item in json_data]
# #     zip_link = json_data[year][qtr]
# #     # print(zip_link)

# #     # for item in json_data:
# #     #     print(item['link'])
# #     #     print(item['year'])
    
# #     try :
# #         time.sleep(0.15)
# #         with requests.get(zip_link, stream=True, headers=headers) as zip_response:
# #          if zip_response.status_code != 200:
# #             raise Exception(f"Failed to download ZIP file; status code: {zip_response.status_code}")

# #         # Write the downloaded content to a temporary file on disk
# #         with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
# #                 for chunk in zip_response.iter_content(chunk_size=4 * 1024 * 1024):  # Using 4MB chunks
# #                     tmp_file.write(chunk)
# #                 temp_zip_path = tmp_file.name
# #                 print(f"Temporary file created: {temp_zip_path}")

# #     # Validate that the downloaded file is a valid ZIP archive
# #                 if not zipfile.is_zipfile(temp_zip_path):
# #                  raise Exception("Downloaded file is not a valid ZIP archive")

# #     # Define the S3 folder path for storing extracted files
# #         temp_folder = f"sec_data/{year}/{qtr}/"
# #         config = TransferConfig(
# #                             multipart_threshold=8 * 1024* 1024,
# #                             max_concurrency=5,
# #                             multipart_chunksize=8 * 1024* 1024,
# #                             use_threads=True
# #                             )

# #         with zipfile.ZipFile(temp_zip_path, 'r') as zip:
# #                 for file_name in zip.namelist():
# #                     with zip.open(file_name) as file_obj:
# #                         s3_key = f"{temp_folder}{file_name.split('.')[0]}.tsv"
                        
# #                         s3_client.upload_fileobj(
# #                             Fileobj =file_obj,
# #                             Bucket = bucket_name,
# #                             Key = s3_key,
# #                             Config = config
# #                         )
# #                         print(f"uploaded file {file_name}")
# #         #


# #     except Exception as e:
# #             print({"error":str(e)})
# #     return "success"


# # if __name__ == "__main__":
#     # test()