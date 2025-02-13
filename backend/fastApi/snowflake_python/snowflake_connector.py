import snowflake.connector as sf
from dotenv import load_dotenv
import os 


load_dotenv()
conn = sf.connect(
    user=os.getenv('SF_USER'),
    password=os.getenv('SF_PASSWORD'),
    account=os.getenv('SF_ACCOUNT'),
    warehouse="DBT_WH",
    database="SEC"
    # schema="RAW"
)
