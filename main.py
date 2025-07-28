from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from readConfig import readConfig
from databaseConnector import databaseConnector
from getSchemaTable import getSchemaTable
from createSparkSchema import createSparkSchema
from readDatabase import readDatabase

def main():
    """
    Main function to run the entire process of reading data from the database.
    Workflow: Config → Schema Info → Type Mapping → Spark Schema → Data Reading
    """
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("DatabaseReader") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Read config
        config = readConfig(config_path="configPostgres.yaml")
        print("Config loaded successfully.")

        # Get database type
        database_type = config['database']['typedb']          
        print(f"Database type: {database_type}")

        # Get tables config
        tables_config = config['tables']
        print(f"Tables: {[table['name'] for table in tables_config]}")
        
        # Create database connection
        jdbc_url, connection_properties = databaseConnector(config)
        print(f"Connection created: {jdbc_url}")
        
        # Dictionary to store DataFrames
        dataframes = {}

        # Process each table
        for table_config in tables_config:
            table_name = table_config['name']
            schema_name = table_config.get('schema', 'public')
            
            print(f"\n{'='*50}")
            print(f"Processing {schema_name}.{table_name}")
            print(f"{'='*50}")
            
            try:
                # Get schema info from database
                print("Getting table schema...")
                table_schema = getSchemaTable(spark, jdbc_url, connection_properties, table_config, database_type)
                
                if not table_schema:
                    print(f"Warning: No schema found for table {table_name}")
                    continue
                
                print(f"Schema info retrieved for {len(table_schema)} columns:")
                for col in table_schema:
                    print(f"- {col['name']}: {col['type']} (nullable: {col['nullable']}) "
                          f"(precision: {col['precision']}) (scale: {col['scale']}) "
                          f"(max_length: {col['max_length']}) (default: {col['default']})")          

                # Create Spark schema from column info
                print("\nCreating Spark schema...")
                spark_schema = createSparkSchema(table_schema)
                
                if not isinstance(spark_schema, StructType):
                    print(f"Error: Invalid Spark schema created for {table_name}")
                    continue
                
                print(f"Spark schema created with {len(spark_schema.fields)} fields:")
                for field in spark_schema.fields:
                    print(f"- {field.name}: {field.dataType} (nullable: {field.nullable})")
                

                # Read data from database with defined schema
                print("\nReading data from database...")
                df = readDatabase(
                    spark=spark,
                    jdbc_url=jdbc_url, 
                    connection_properties=connection_properties, 
                    table_config=table_config, 
                    spark_schema=spark_schema, 
                    query=None
                )
                
                # Check if DataFrame is valid
                if df is None:
                    print(f"Error: Failed to read data from {table_name}")
                    continue
                
                # Get row count (cache for performance)
                df.cache()
                row_count = df.count()
                print(f"Data read successfully. Row count: {row_count}")
                
                # Store DataFrame in dictionary
                dataframes[table_name] = df
                
                # Show sample data
                if row_count > 0:
                    print(f"\nSample data from {table_name}:")
                    df.show(3, truncate=False)
                else:
                    print(f"No data found in {table_name} with the given criteria")
                
            except Exception as table_error:
                print(f"Error processing table {table_name}: {str(table_error)}")
                import traceback
                traceback.print_exc()
                continue  # Continue with next table instead of stopping

        print(f"\n{'='*50}")
        print("PROCESSING COMPLETED")
        print(f"{'='*50}")
        print(f"Successfully processed {len(dataframes)} tables:")
        for table_name, df in dataframes.items():
            print(f"- {table_name}: {df.count()} rows")
        
        return dataframes
        
    except Exception as e:
        print(f"Critical Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
    
    finally:
        print("\nStopping Spark session...")
        spark.stop()

if __name__ == "__main__":
    result = main()
    if result:
        print(f"\nProgram completed successfully with {len(result)} DataFrames")
    else:
        print("\nProgram completed with errors")