from fastapi import FastAPI, HTTPException, Query, Body, Path
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from csv_data_ingestion import CsvDataIngestion

app = FastAPI(title="CSV Data Ingestion API")

TABLE_NAME = "csv_data_table01"
# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('HOME_PROD_ENV_STAGING_POSTGRES_DB'),
    'user': os.getenv('HOME_PROD_ENV_STAGING_POSTGRES_USER'),
    'password': os.getenv('HOME_PROD_ENV_STAGING_POSTGRES_PASSWORD'),
    'host': os.getenv('HOME_PROD_ENV_STAGING_POSTGRES_HOST'),
    'port': int(os.getenv('HOME_PROD_ENV_STAGING_POSTGRES_PORT')),
    'schema': os.getenv('HOME_PROD_ENV_STAGING_POSTGRES_TEST_SCHEMA')
}

# Create SQLAlchemy engine
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
engine = create_engine(DATABASE_URL)

# Pydantic models
class SearchQuery(BaseModel):
    conditions: Dict[str, Any]
    operator: str = "AND"
    case_sensitive: bool = False

class UpdateItem(BaseModel):
    columns: Dict[str, Any] = Field(..., description="Columns to update and their new values")

class BatchDeleteRequest(BaseModel):
    ids: List[int] = Field(..., description="List of IDs to delete")

# Initialize CsvDataIngestion
csv_ingestion = CsvDataIngestion(
    db_config=DB_CONFIG,
    table_name=TABLE_NAME
)

def build_where_clause(conditions: Dict[str, Any], operator: str = "AND", case_sensitive: bool = False) -> tuple:
    """Helper function to build WHERE clause and parameters"""
    where_clauses = []
    params = {}
    
    for idx, (col, value) in enumerate(conditions.items()):
        param_name = f"param_{idx}"
        if isinstance(value, str) and not case_sensitive:
            where_clauses.append(f'LOWER("{col}") = LOWER(:{param_name})')
        else:
            where_clauses.append(f'"{col}" = :{param_name}')
        params[param_name] = value

    where_statement = f" {operator} ".join(where_clauses)
    return where_statement, params

@app.post("/query_by_text")
async def query_by_text(
    search_query: SearchQuery,
    limit: int = Query(10, ge=1, le=100)
):
    """Search for items using text-based conditions"""
    try:
        where_clause, params = build_where_clause(
            search_query.conditions,
            search_query.operator,
            search_query.case_sensitive
        )
        
        query = text(f"""
            SELECT * 
            FROM {DB_CONFIG['schema']}.{csv_ingestion.table_name}
            WHERE {where_clause}
            LIMIT :limit
        """)
        params['limit'] = limit

        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)
            return df.to_dict(orient='records')
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/query_similar/{column}/{value}")
async def query_similar(
    column: str = Path(..., description="Column to search in"),
    value: str = Path(..., description="Value to find similar matches for"),
    limit: int = Query(10, ge=1, le=100)
):
    """Find similar matches using LIKE query"""
    try:
        query = text(f"""
            SELECT * 
            FROM {DB_CONFIG['schema']}.{csv_ingestion.table_name}
            WHERE "{column}" ILIKE :pattern
            LIMIT :limit
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'pattern': f'%{value}%', 'limit': limit})
            return df.to_dict(orient='records')
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/items")
async def delete_items(request: BatchDeleteRequest):
    """Delete multiple items by their IDs"""
    try:
        ids_str = ','.join(str(id) for id in request.ids)
        query = text(f"""
            DELETE FROM {DB_CONFIG['schema']}.{csv_ingestion.table_name}
            WHERE _csv_import_id_ IN ({ids_str})
            RETURNING _csv_import_id_
        """)
        
        with engine.begin() as conn:
            result = conn.execute(query)
            deleted_ids = [row[0] for row in result]
            return {"deleted_ids": deleted_ids}
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/items/{item_id}")
async def get_item(item_id: int = Path(..., ge=1)):
    """Get a single item by ID"""
    try:
        query = text(f"""
            SELECT * 
            FROM {DB_CONFIG['schema']}.{csv_ingestion.table_name}
            WHERE _csv_import_id_ = :item_id
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'item_id': item_id})
            if df.empty:
                raise HTTPException(status_code=404, detail="Item not found")
            return df.to_dict(orient='records')[0]
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.put("/items/{item_id}")
async def update_item(
    item_id: int = Path(..., ge=1),
    update_data: UpdateItem = Body(...)
):
    """Update an item by ID"""
    try:
        set_clauses = [f'"{k}" = :{k}' for k in update_data.columns.keys()]
        set_clause = ", ".join(set_clauses)
        
        query = text(f"""
            UPDATE {DB_CONFIG['schema']}.{csv_ingestion.table_name}
            SET {set_clause}
            WHERE _csv_import_id_ = :item_id
            RETURNING *
        """)
        
        params = {**update_data.columns, 'item_id': item_id}
        
        with engine.begin() as conn:
            result = conn.execute(query, params)
            updated_row = result.fetchone()
            if not updated_row:
                raise HTTPException(status_code=404, detail="Item not found")
            return dict(zip(result.keys(), updated_row))
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/items")
async def create_item(data: Dict[str, Any] = Body(...)):
    """Create a new item"""
    try:
        columns = list(data.keys())
        values_clause = ",".join(f":{col}" for col in columns)
        columns_str = ",".join(f'"{col}"' for col in columns)
        
        query = text(f"""
            INSERT INTO {DB_CONFIG['schema']}.{csv_ingestion.table_name}
            ({columns_str})
            VALUES ({values_clause})
            RETURNING *
        """)
        
        with engine.begin() as conn:
            result = conn.execute(query, data)
            new_row = result.fetchone()
            return dict(zip(result.keys(), new_row))
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)