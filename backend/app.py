from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from utils.snowflake.core import get_sf_client, execute_query_async
import pandas as pd
from dotenv import load_dotenv

app = FastAPI()
load_dotenv()

class QueryRequest(BaseModel):
    query: str

@app.get('/')
async def welcome():
    return 'welcome to / endpoint'

@app.get('/schema')
async def info_schema(sf_schema : str):
    if sf_schema.lower() in ['raw','json','dw']:
        return 'good request'
    return HTTPException(status_code=400, detail='Bad Request')


@app.post("/execute_query/")
async def run_query(request: QueryRequest):
    conn = get_sf_client()
    result = await execute_query_async(conn, request.query)
    if result["success"]:
        return {"records": result["data"]}
    else:
        raise HTTPException(status_code=400, detail=result["error"])