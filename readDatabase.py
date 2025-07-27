def readDatabase(spark, jdbc_url, connection_properties, table_config, query=None, spark_schema=None):
    """
    Read data from the database using Spark JDBC.
    Args:
        spark: SparkSession
        jdbc_url: JDBC connection URL
        connection_properties: dict with connection properties
        table_config: dict with table configuration
        query: optional custom SQL query
        spark_schema: optional Spark schema
    Returns:
        DataFrame: Spark DataFrame with the data
    """
    schema_name = table_config.get('schema', 'public')
    table_name = table_config['name']
    
    if query:
        # If a custom query is provided
        dbtable = f"({query}) as custom_query"
    else:
        # Read the entire table
        dbtable = f"{schema_name}.{table_name}"
    
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", dbtable) \
        .option("user", connection_properties['user']) \
        .option("password", connection_properties['password']) \
        .option("driver", connection_properties['driver']) \
        .schema(spark_schema) \
        .load()
    
    return df