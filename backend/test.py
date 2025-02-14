from utils.snowflake.core import get_sf_client, select
from dotenv import load_dotenv
import os

load_dotenv()

conn = get_sf_client()
cur = conn.cursor()
cur.execute("USE DATABASE SEC")
cur.execute("USE SCHEMA JSON")
query="select * from json.sub limit 10"

