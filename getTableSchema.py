import psycopg2
from typing import List, Tuple

def getTableSchema(config: dict) -> List[Tuple[str, str, str]]:
    """
    Fetch column metadata (name, type, nullability) for 'public.accounts' table from PostgreSQL.

    Args:
        config (dict): Dictionary containing PostgreSQL config under 'postgres' key.

    Returns:
        List[Tuple[column_name, data_type, is_nullable]]
    """
    pg_config = config["postgres"]

    conn = psycopg2.connect(
        dbname=pg_config["dbname"],
        user=pg_config["user"],
        password=pg_config["password"],
        host=pg_config["host"],
        port=pg_config["port"]
    )

    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'accounts'
        """)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()
