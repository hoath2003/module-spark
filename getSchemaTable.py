def getSchemaTable(spark, jdbc_url, connection_properties, table_config, database_type):
    """
    Retrieve the schema of a table from the database and return detailed information.
    
    Args:
        spark: Spark session
        jdbc_url: JDBC connection URL
        connection_properties: Connection properties dict
        table_config: Table configuration (dict or string)
        database_type: Type of database ('oracle', 'postgresql', 'mysql', 'sqlserver', 'sqlite')
    
    Returns:
        list of dicts with format:
        [
            {
                'name': 'col_name',
                'type': 'data_type',
                'nullable': bool,
                'precision': int or None,
                'scale': int or None,
                'max_length': int or None,
                'default': str or None
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
    
    # Generate metadata query based on database type
    metadata_query = getMetadataQuery(database_type, schema_name, table_name)
    
    schema_table = []
    try:
        # Execute metadata query using Spark JDBC
        metadata_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"({metadata_query})") \
            .option("user", connection_properties['user']) \
            .option("password", connection_properties['password']) \
            .option("driver", connection_properties['driver']) \
            .load()
        
        # Convert DataFrame to list of dicts in the appropriate format
        for row in metadata_df.collect():
            column_info = parseColumnInfo(row, database_type)
            schema_table.append(column_info)
        
        return schema_table
        
    except Exception as e:
        print(f"Error retrieving schema for {database_type}: {e}")
        return schema_table


def getMetadataQuery(database_type, schema_name, table_name):
    """Generate metadata query based on database type."""
    
    if database_type.lower() == 'oracle':
        return f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            NULLABLE,
            DATA_LENGTH,
            DATA_PRECISION,
            DATA_SCALE,
            DATA_DEFAULT
        FROM all_tab_columns 
        WHERE owner = '{schema_name.upper()}' 
        AND table_name = '{table_name.upper()}'
        ORDER BY column_id
        """
    
    elif database_type.lower() == 'postgresql':
        return f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            column_default
        FROM information_schema.columns 
        WHERE table_schema = '{schema_name.lower()}' 
        AND table_name = '{table_name.lower()}'
        ORDER BY ordinal_position
        """
    
    elif database_type.lower() == 'mysql':
        return f"""
        SELECT 
            COLUMN_NAME as column_name,
            DATA_TYPE as data_type,
            IS_NULLABLE as is_nullable,
            CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
            NUMERIC_PRECISION as numeric_precision,
            NUMERIC_SCALE as numeric_scale,
            COLUMN_DEFAULT as column_default
        FROM information_schema.columns 
        WHERE table_schema = '{schema_name}' 
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
    
    elif database_type.lower() == 'sqlserver':
        return f"""
        SELECT 
            c.COLUMN_NAME as column_name,
            c.DATA_TYPE as data_type,
            c.IS_NULLABLE as is_nullable,
            c.CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
            c.NUMERIC_PRECISION as numeric_precision,
            c.NUMERIC_SCALE as numeric_scale,
            c.COLUMN_DEFAULT as column_default
        FROM information_schema.columns c
        WHERE c.TABLE_SCHEMA = '{schema_name}' 
        AND c.TABLE_NAME = '{table_name}'
        ORDER BY c.ordinal_position
        """
    
    elif database_type.lower() == 'sqlite':
        return f"""
        PRAGMA table_info({table_name})
        """
    
    else:
        raise ValueError(f"Unsupported database type: {database_type}")


def parseColumnInfo(row, database_type):
    """Parse column information based on database type."""
    
    if database_type.lower() == 'oracle':
        # Handle nullable - Oracle uses 'NULLABLE' column (uppercase)
        nullable = True if str(row['NULLABLE']).upper() == 'Y' else False
        
        # Handle precision/scale (convert to int or None)
        precision = int(row['DATA_PRECISION']) if row['DATA_PRECISION'] is not None else None
        
        # Handle scale (convert to int or None)
        if row['DATA_SCALE'] is None:
            scale = None
        else:
            try:
                scale = int(row['DATA_SCALE'])
            except Exception:
                scale = None
        
        return {
            'name': row['COLUMN_NAME'],
            'type': row['DATA_TYPE'],
            'nullable': nullable,
            'max_length': row['DATA_LENGTH'],
            'precision': precision,
            'scale': scale,
            'default': row['DATA_DEFAULT']
        }
    
    elif database_type.lower() in ['postgresql', 'mysql', 'sqlserver']:
        # Handle nullable - PostgreSQL/MySQL/SQL Server use 'is_nullable'
        nullable = True if str(row['is_nullable']).upper() == 'YES' else False
        
        # Handle precision/scale
        precision = int(row['numeric_precision']) if row['numeric_precision'] is not None else None
        scale = int(row['numeric_scale']) if row['numeric_scale'] is not None else None
        
        return {
            'name': row['column_name'],
            'type': row['data_type'],
            'nullable': nullable,
            'max_length': row['character_maximum_length'],
            'precision': precision,
            'scale': scale,
            'default': row['column_default']
        }
    
    elif database_type.lower() == 'sqlite':
        # SQLite PRAGMA table_info returns different column structure
        # cid, name, type, notnull, dflt_value, pk
        nullable = not bool(row['notnull']) if 'notnull' in row else True
        
        return {
            'name': row['name'],
            'type': row['type'],
            'nullable': nullable,
            'max_length': None,  # SQLite doesn't provide max_length in PRAGMA
            'precision': None,   # SQLite doesn't provide precision in PRAGMA
            'scale': None,       # SQLite doesn't provide scale in PRAGMA
            'default': row['dflt_value'] if 'dflt_value' in row else None
        }
    
    else:
        raise ValueError(f"Unsupported database type: {database_type}")
