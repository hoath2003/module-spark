# buildSparkSchema.py - Provides function to convert PostgreSQL schema rows to Spark StructType schema

from pyspark.sql.types import StructType, StructField
from typing import List, Tuple
from mapPostgresTypeToSpark import mapPostgresTypeToSpark

def buildSparkSchema(schema_rows: List[Tuple[str, str, str]]) -> StructType:
    """
    Convert PostgreSQL schema rows to Spark StructType.
    Args:
        schema_rows (List[Tuple[str, str, str]]): List of (column_name, postgres_type, is_nullable)
    Returns:
        StructType: Spark schema
    """
    fields = []
    # Map each Postgres column to Spark StructField
    for name, pg_type, nullable in schema_rows:
        spark_type = mapPostgresTypeToSpark(pg_type)
        fields.append(StructField(name, spark_type, nullable.upper() == "YES"))
    return StructType(fields)
