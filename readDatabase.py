# readDatabase.py - Provides function to load a PostgreSQL table into a Spark DataFrame using a custom schema

from pyspark.sql import SparkSession, DataFrame
from buildSparkSchema import buildSparkSchema
from getTableSchema import getTableSchema

def readDatabase(spark: SparkSession, config: dict) -> DataFrame:
    """
    Read a PostgreSQL table into a Spark DataFrame using JDBC and schema from Postgres.
    Args:
        spark (SparkSession): Active Spark session.
        config (dict): Loaded config with PostgreSQL connection info.
    Returns:
        DataFrame: Spark DataFrame loaded from PostgreSQL.
    """
    # Extract connection and table info from config
    pg_conf = config["postgres"]
    table_name = config["table"]["name"]
    schema_name = config["table"]["schema"]
    
    try:
        # Get schema from PostgreSQL and build Spark schema
        getSchema = getTableSchema(config)
        spark_schema = buildSparkSchema(getSchema)
        
        # Read table from PostgreSQL using JDBC
        return spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{pg_conf['host']}:{pg_conf['port']}/{pg_conf['dbname']}") \
            .option("dbtable", f"{schema_name}.{table_name}") \
            .option("user", pg_conf["user"]) \
            .option("password", pg_conf["password"]) \
            .schema(spark_schema) \
            .load()
    except Exception as e:
        print(f"Error reading database: {e}")
        raise
