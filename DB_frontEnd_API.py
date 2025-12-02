from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Literal

app = FastAPI()

class QueryRequest(BaseModel):
    query: str

def parse_kv_string(kv_string: str):
    pairs = kv_string.split(',')
    kv = {}
    for pair in pairs:
        if '=' not in pair:
            raise ValueError(f"Invalid pair: {pair}")
        key, value = pair.split('=', 1)
        kv[key.strip()] = value.strip()
    return kv

def format_value(value: str):
    if value.replace('.', '', 1).isdigit():
        return value
    return f"'{value}'"

def generate_insert(table: str, kv: dict):
    columns = ', '.join(kv.keys())
    values = ', '.join(format_value(v) for v in kv.values())
    return f"INSERT INTO {table} ({columns}) VALUES ({values});"

def generate_update(table: str, kv: dict):
    if 'id' not in kv:
        raise ValueError("UPDATE requires 'id' as primary key")
    id_val = kv.pop('id')
    set_clause = ', '.join(f"{k}={format_value(v)}" for k, v in kv.items())
    return f"UPDATE {table} SET {set_clause} WHERE id={id_val};"

def generate_delete(table: str, kv: dict):
    if 'id' not in kv:
        raise ValueError("DELETE requires 'id' as primary key")
    return f"DELETE FROM {table} WHERE id={kv['id']};"

@app.post("/generate-sql")
def generate_sql(req: QueryRequest):
    try:
        parts = req.query.strip().split('|')
        if len(parts) != 3:
            raise ValueError("Query must have 3 parts: ACTION|Table|key=value,...")

        action, table, kv_string = parts
        action = action.upper()
        kv = parse_kv_string(kv_string)

        if action == "INSERT":
            sql = generate_insert(table, kv)
        elif action == "UPDATE":
            sql = generate_update(table, kv)
        elif action == "DELETE":
            sql = generate_delete(table, kv)
        else:
            raise ValueError(f"Unsupported action: {action}")

        return {"sql": sql}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))