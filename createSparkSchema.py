from pyspark.sql.types import StructType, StructField
from mapDatabaseTypeToSparkType import mapDatabaseTypeToSparkType

def createSparkSchema(schema_table):
    """
    Create a Spark schema from the database columns information.
    Args:
        column_info_list: list of dicts containing column information.
            Example: [{'name': 'id', 'type': 'integer', 'nullable': False}, ...]
    Returns:
        StructType: Spark schema
    """
    fields = []
    
    for col_info in schema_table:
        column_name = col_info['name']
        db_type = col_info['type']
        # Always set nullable=True to avoid schema mismatch errors with Spark JDBC
        nullable = True
        
        # Get precision and scale if available
        precision = col_info.get('precision')
        scale = col_info.get('scale')
        # Get max_length if available (prefer 'max_length', fallback to 'length')
        max_length = col_info.get('max_length')
        if max_length is None:
            max_length = col_info.get('length')
        # Map database type to Spark type with precision/scale/max_length
        spark_type = mapDatabaseTypeToSparkType(db_type, precision, scale, max_length)
        
        field = StructField(column_name, spark_type, nullable)
        fields.append(field)
    
    return StructType(fields)