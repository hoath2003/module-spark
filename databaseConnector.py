def databaseConnector(config):
    """
    Create connection string and properties for Spark JDBC connection.
    Args:
        config: Configuration dictionary containing database info
    Returns:
        tuple: (jdbc_url, connection_properties)
    Raises:
        ValueError: If the database type is unsupported
    """
    db_config = config['database']
    
    # Mapping database types to JDBC URLs
    db_type_mapping = {
        'postgresql': {
            'driver': 'org.postgresql.Driver',
            'url_template': 'jdbc:postgresql://{host}:{port}/{dbname}'
        },
        'mysql': {
            'driver': 'com.mysql.cj.jdbc.Driver',
            'url_template': 'jdbc:mysql://{host}:{port}/{dbname}'
        },
        'oracle': {
            'driver': 'oracle.jdbc.driver.OracleDriver',
            'url_template': 'jdbc:oracle:thin:@{host}:{port}/{dbname}'
        },
        'sqlserver': {
            'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'url_template': 'jdbc:sqlserver://{host}:{port};databaseName={dbname}'
        }
    }
    
    db_type = db_config['typedb'].lower()
    
    if db_type not in db_type_mapping:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    db_info = db_type_mapping[db_type]
    
    # Create JDBC URL
    jdbc_url = db_info['url_template'].format(
        host=db_config['host'],
        port=db_config['port'],
        dbname=db_config['dbname']
    )
    
    # Create connection properties
    connection_properties = {
        'user': db_config['user'],
        'password': db_config['password'],
        'driver': db_info['driver']
    }
    
    return jdbc_url, connection_properties