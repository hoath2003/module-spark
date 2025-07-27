from pyspark.sql import SparkSession
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
        .getOrCreate()
    
    try:
        # Read config
        config = readConfig()
        print("Config loaded successfully.")
        print(f"Database: {config['database']['typedb']}")
        print(f"Tables: {[table['name'] for table in config['tables']]}")
        
        # Create database connection
        jdbc_url, connection_properties = databaseConnector(config)
        print(f"Connection created: {jdbc_url}")
        
        # Dictionary to store DataFrames
        dataframes = {}

        # Process each table
        for table_config in config['tables']:
            table_name = table_config['name']
            schema_name = table_config.get('schema', 'public')
            
            print(f"Processing {schema_name}.{table_name}")
            
            # Get schema info from database
            table_schema = getSchemaTable(spark, jdbc_url, connection_properties, table_config)
            print(f"Schema info retrieved for {len(table_schema)} columns:")
            for col in table_schema:
                print(f"- {col['name']}: {col['type']} (nullable: {col['nullable']}) (precision: {col['precision']}) (scale: {col['scale']}) (max_length: {col['max_length']})")
            
            # Create Spark schema from column info
            spark_schema = createSparkSchema(table_schema)
            print(f"Spark schema created with {len(spark_schema.fields)} fields:")
            for field in spark_schema.fields:
                print(f"- {field.name}: {field.dataType} (nullable: {field.nullable})")
            
            # Read data from database with defined schema
            df = readDatabase(spark, jdbc_url, connection_properties, table_config, spark_schema=spark_schema)
            row_count = df.count()
            print(f"Data read successfully. Row count: {row_count}")
            
            # Store DataFrame in dictionary
            dataframes[table_name] = df
            print(f"Dataframes: {dataframes}")
            
            # Show sample data
            print(f"Sample data {table_name}:")
            df.show(3, truncate=False)
        
        print("Completed.")
        print(f"Processed {len(dataframes)} tables:")
        for table_name, df in dataframes.items():
            print(f"- {table_name}: {df.count()} rows")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
