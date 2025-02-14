import os
import asyncio
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError


def get_sf_client():
    USER = os.getenv('SF_USER')
    PASSWORD = os.getenv('SF_PWD')
    ACCOUNT = os.getenv('SF_ACCOUNT')
    WAREHOUSE = os.getenv('SF_WAREHOUSE')
    DATABASE = os.getenv('DATABASE')    
    SCHEMA = os.getenv('SCHEMA')
        
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        role='DBT_ROLE'
        )
    
    return conn


def execute_query_sync(conn, query: str):
    try:
        cur = conn.cursor()
        cur.execute(query)

        # Fetch results and convert to list of dictionaries
        columns = [desc[0] for desc in cur.description] if cur.description else []
        results = [dict(zip(columns, row)) for row in cur.fetchall()] if columns else []

        cur.close()
        conn.close()
        return {"success": True, "data": results}

    except snowflake.connector.errors.ProgrammingError as e:
        return {"success": False, "error": str(e)}

# Async function to execute query using asyncio
async def execute_query_async(conn, query: str):
    return await asyncio.to_thread(execute_query_sync, conn, query)