import snowflake.connector
from dotenv import load_dotenv
import os 


load_dotenv()
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SF_ACCOUNT'),
    warehouse="DBT_WH",
    database="SEC",
    schema="RAW"
)