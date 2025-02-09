import os 
from dotenv import load_dotenv
import json
from utils.aws import s3
from io import BytesIO
import requests
from firecrawl import FirecrawlApp
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



if __name__ == "__main__":
    test()
