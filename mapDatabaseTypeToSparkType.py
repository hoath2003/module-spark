import re
from pyspark.sql.types import *
import warnings

def mapDatabaseTypeToSparkType(db_type_name, precision=None, scale=None, max_length=None):
    """
    Map database data types to Spark data types (comprehensive, banking-focused).
    Args:
        db_type_name: Name of the type from the database (may include params, e.g. varchar(255))
        precision: Precision for numeric types (optional, overrides parsed)
        scale: Scale for numeric types (optional, overrides parsed)
        max_length: Optional maximum length for string types (overrides length in db_type_name if smaller)
    Returns:
        Corresponding Spark SQL type
    """
    # Normalize and extract type name and params
    normalized = db_type_name.lower().strip()
    match = re.match(r"([a-zA-Z0-9_ ]+)(\\((.*?)\\))?", normalized)
    base_type = match.group(1).strip() if match else normalized
    params = match.group(3).split(',') if match and match.group(3) else []
    params = [p.strip() for p in params]

    # Parse precision/scale/length if present
    parsed_precision = int(params[0]) if params and params[0].isdigit() else None
    parsed_scale = int(params[1]) if len(params) > 1 and params[1].isdigit() else None
    length = parsed_precision
    # Allow explicit override
    precision = precision if precision is not None else parsed_precision
    scale = scale if scale is not None else parsed_scale

    # Determine effective string length
    effective_length = None
    if base_type in ['varchar', 'char', 'nvarchar', 'nchar', 'varchar2', 'nvarchar2']:
        if max_length is not None and length is not None:
            if max_length < length:
                warnings.warn(f"max_length ({max_length}) is less than declared length ({length}) in type '{db_type_name}'. Using max_length.")
                effective_length = max_length
            elif max_length > length:
                warnings.warn(f"max_length ({max_length}) is greater than declared length ({length}) in type '{db_type_name}'. Using declared length.")
                effective_length = length
            else:
                effective_length = length
        elif max_length is not None:
            effective_length = max_length
        elif length is not None:
            effective_length = length

    # Aliases and variants
    alias_map = {
        'int4': 'integer',
        'int8': 'bigint',
        'int2': 'smallint',
        'serial': 'integer',
        'bigserial': 'bigint',
        'bool': 'boolean',
        'number': 'decimal',  # Oracle
        'bpchar': 'char',
        'bit': 'boolean',
        'binary': 'binary',
        'varbinary': 'binary',
        'uuid': 'uuid',
        'jsonb': 'json',
        'json': 'json',
        'bytea': 'binary',
        'blob': 'binary',
        'clob': 'string',
        'xml': 'string',
        'enum': 'string',
        'geography': 'string',
        'geometry': 'string',
        'array': 'array',
    }
    base_type = alias_map.get(base_type, base_type)

    type_mapping = {
        # Numeric
        'integer': IntegerType(),
        'bigint': LongType(),
        'smallint': ShortType(),
        'real': FloatType(),
        'float': FloatType(),
        'double': DoubleType(),
        'double precision': DoubleType(),
        'decimal': lambda p, s: DecimalType(p or 38, s if s is not None else 18),
        'numeric': lambda p, s: DecimalType(p or 38, s if s is not None else 18),
        'money': DecimalType(19, 4),
        'number': lambda p, s: DecimalType(p or 38, s if s is not None else 18),
        # String
        'varchar': StringType(),
        'char': StringType(),
        'nvarchar': StringType(),
        'nchar': StringType(),
        'varchar2': StringType(),
        'nvarchar2': StringType(),
        'text': StringType(),
        'clob': StringType(),
        'enum': StringType(),
        # Date/Time
        'date': DateType(),
        'timestamp': TimestampType(),
        'datetime': TimestampType(),
        'datetime2': TimestampType(),
        'time': StringType(),
        # Boolean
        'boolean': BooleanType(),
        'bit': BooleanType(),
        # Binary
        'binary': BinaryType(),
        'blob': BinaryType(),
        'bytea': BinaryType(),
        # Special
        'uuid': StringType(),
        'json': StringType(),
        'xml': StringType(),
        'geography': StringType(),
        'geometry': StringType(),
        # Array (fallback to StringType for Spark SQL)
        'array': StringType(),
    }

    # DECIMAL/NUMERIC/NUMBER types
    if base_type in ['decimal', 'numeric', 'number']:
        return DecimalType(precision or 38, scale if scale is not None else 18)
    # Types with length (e.g. varchar(255))
    if base_type in ['varchar', 'char', 'nvarchar', 'nchar', 'varchar2', 'nvarchar2'] and (length or max_length):
        if effective_length is not None:
            warnings.warn(f"Type '{base_type}' with effective length {effective_length} detected for '{db_type_name}'. Spark StringType does not enforce length, but length is {effective_length}.")
        return StringType()  # Spark does not enforce length
    # Array/JSON/XML/UUID/ENUM/GEOMETRY
    if base_type in type_mapping:
        mapped = type_mapping[base_type]
        if callable(mapped):
            return mapped(precision, scale)
        return mapped
    # Fallback
    warnings.warn(f"Unknown or unmapped database type: {db_type_name}. Defaulting to StringType.")
    return StringType()