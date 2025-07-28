from pyspark.sql.types import StructType
def readDatabase(spark, jdbc_url, connection_properties, table_config, query=None, spark_schema=None):
    """
    Read data from the database using Spark JDBC.
    
    Args:
        spark: SparkSession
        jdbc_url: JDBC connection URL
        connection_properties: dict with connection properties
        table_config: dict with table configuration
        query: optional custom SQL query
        spark_schema: optional Spark schema (StructType)
        
    Returns:
        DataFrame: Spark DataFrame with the data
    """
    try:
        schema_name = table_config.get('schema', 'public')
        table_name = table_config['name']
        
        if query:
            # If a custom query is provided, wrap it properly
            dbtable = f"({query}) as custom_query"
        else:
            # Read the entire table
            dbtable = f"{schema_name}.{table_name}"
        
        print(f"Executing query: {dbtable}")
        
        # Build the JDBC reader
        jdbc_reader = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", dbtable) \
            .option("user", connection_properties['user']) \
            .option("password", connection_properties['password']) \
            .option("driver", connection_properties['driver'])
        
        # Add schema if provided
        if spark_schema and isinstance(spark_schema, StructType):
            jdbc_reader = jdbc_reader.schema(spark_schema)
        
        # Load the data
        df = jdbc_reader.load()
        
        return df
        
    except Exception as e:
        print(f"Error in readDatabase: {str(e)}")
        raise e