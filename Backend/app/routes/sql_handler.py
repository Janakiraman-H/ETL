from fastapi import FastAPI, HTTPException
from typing import List
from app.utils.validation import validate_sql
from app.models.oracle import execute_sql, get_column_names

app = FastAPI()

@app.post("/validate_sql/")
async def validate_sql_endpoint(sql_query: str):
    if not sql_query:
        raise HTTPException(status_code=400, detail="SQL query cannot be empty")
    
    # Validate SQL query
    is_valid, error_msg = validate_sql(sql_query)
    if not is_valid:
        raise HTTPException(status_code=400, detail=error_msg)

    # Execute SQL query against OracleDB
    result_data = execute_sql(sql_query)

    # Get column names
    column_names = get_column_names(result_data)

    return {"column_names": column_names}
