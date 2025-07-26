# getTableSchema.py - Provides function to fetch table schema (column names, types, nullability) from PostgreSQL

import psycopg2
from typing import List, Tuple

def getTableSchema(config: dict) -> List[Tuple[str, str, str]]:
    """
    Fetch column metadata (name, type, nullability) for a given table from PostgreSQL.
    Args:
        config (dict): Dictionary containing PostgreSQL config under 'postgres' key, and 'table'.
    Returns:
        List[Tuple[column_name, data_type, is_nullable]]
    """
    # Extract connection and table info from config
    pg_conf = config["postgres"]
    table_name = config["table"]["name"]
    schema_name = config["table"]["schema"]
    
    try:
        # Connect to PostgreSQL and fetch schema info
        conn = psycopg2.connect(
            dbname=pg_conf["dbname"],
            user=pg_conf["user"],
            password=pg_conf["password"],
            host=pg_conf["host"],
            port=pg_conf["port"]
        )
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (schema_name, table_name))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching table schema: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
