# mapPostgresTypeToSpark.py - Provides function to map PostgreSQL data types to Spark SQL data types

from pyspark.sql.types import (
    StringType, IntegerType, LongType, ShortType, FloatType,
    DoubleType, DecimalType, BooleanType, DateType,
    TimestampType, BinaryType
)

def mapPostgresTypeToSpark(postgres_type: str):
    """
    Map a PostgreSQL data type string to a Spark SQL data type object.
    Args:
        postgres_type (str): PostgreSQL data type name
    Returns:
        Corresponding Spark SQL data type object
    """
    # Dictionary mapping Postgres types to Spark types
    mapping = {
        "integer": IntegerType(),
        "bigint": LongType(),
        "smallint": ShortType(),
        "serial": IntegerType(),
        "bigserial": LongType(),
        "decimal": DecimalType(38, 18),
        "numeric": DecimalType(38, 18),
        "real": FloatType(),
        "double precision": DoubleType(),
        "money": DecimalType(38, 18),
        "varchar": StringType(),
        "character varying": StringType(),
        "char": StringType(),
        "character": StringType(),
        "text": StringType(),
        "boolean": BooleanType(),
        "bool": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "timestamp without time zone": TimestampType(),
        "timestamp with time zone": TimestampType(),
        "time": StringType(),
        "json": StringType(),
        "jsonb": StringType(),
        "uuid": StringType(),
        "bytea": BinaryType()
    }
    # Return mapped Spark type or StringType as default
    return mapping.get(postgres_type.lower(), StringType())
