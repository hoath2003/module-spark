# main.py - Entry point for loading data from PostgreSQL into a Spark DataFrame
# 1. Load configuration from YAML file
# 2. Initialize SparkSession
# 3. Read data from PostgreSQL into Spark DataFrame and display results

import yaml
from pyspark.sql import SparkSession
from readDatabase import readDatabase

try:
    # Load database and table configuration from YAML file
    with open("configDB.yaml", "r") as f:
        config = yaml.safe_load(f)
    print("Config loaded successfully")
    print(f"Table: {config['table']['schema']}.{config['table']['name']}")
    print(f"Database: {config['postgres']['dbname']} on {config['postgres']['host']}:{config['postgres']['port']}")
except Exception as e:
    print(f"Error loading configDB.yaml: {e}")
    exit(1)

try:
    # Initialize SparkSession
    spark = SparkSession.builder.appName("ReadPostgres").getOrCreate()
    print("Spark session created")
    
    # Read data from PostgreSQL into Spark DataFrame
    df = readDatabase(spark, config)
    print("Data loaded successfully")
    
    # Display schema and data
    print("\nSchema:")
    df.printSchema()
    print("\nData:")
    df.show()
    
except Exception as e:
    print(f"Error running Spark job: {e}")
    exit(1)
