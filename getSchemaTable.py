def getSchemaTable(spark, jdbc_url, connection_properties, table_config):
    """
    Retrieve the schema of a table from the database and return detailed information.
    Returns:
        list of dicts with format:
        [
            {
                'name': 'col_name',
                'type': 'db_type',
                'nullable': bool,
                'precision': int or None,
                'scale': int or None
            },
            ...
        ]
    """
    if isinstance(table_config, dict):
        schema_name = table_config.get('schema', 'public')
        table_name = table_config['name']
    else:
        schema_name = 'public'
        table_name = table_config
    
    # Query to get metadata from information_schema
    metadata_query = f"""
    SELECT 
        column_name,
        data_type,
        is_nullable,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        column_default
    FROM information_schema.columns 
    WHERE table_schema = '{schema_name}' 
    AND table_name = '{table_name}'
    ORDER BY ordinal_position
    """
    
    schema_table = []
    try:
        # Retrieve detailed metadata
        metadata_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"({metadata_query}) as metadata") \
            .option("user", connection_properties['user']) \
            .option("password", connection_properties['password']) \
            .option("driver", connection_properties['driver']) \
            .load()
        
        # Convert DataFrame to list of dicts in the appropriate format
        for row in metadata_df.collect():
            # Handle nullable
            nullable = True if str(row['is_nullable']).upper() == 'YES' else False
            # Handle precision/scale (convert to int or None, scale=0 phải là int(0))
            precision = int(row['numeric_precision']) if row['numeric_precision'] is not None else None
            # Sửa đoạn này để scale=0 không bị bỏ qua
            if row['numeric_scale'] is None:
                scale = None
            else:
                try:
                    scale = int(row['numeric_scale'])
                except Exception:
                    scale = None
            column_info = {
                'name': row['column_name'],
                'type': row['data_type'],
                'nullable': nullable,
                'max_length': row['character_maximum_length'],
                'precision': precision,
                'scale': scale,
                'default': row['column_default']
            }
            schema_table.append(column_info)
        
        return schema_table
        
    except Exception as e:
        print(f"Error retrieving schema: {e}")
        return schema_table