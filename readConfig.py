import yaml

def readConfig(config_path="config.yaml"):
    """
    Read the config file and return a dictionary containing configuration information.
    Args:
        config_path: Path to the config YAML file (default: 'config.yaml')
    Returns:
        dict: Configuration dictionary
    Raises:
        FileNotFoundError: If the config file is not found
        ValueError: If YAML parsing fails or required sections are missing
        Exception: For other errors
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        
        # Check and validate config
        if 'database' not in config:
            raise ValueError("Missing 'database' section in config")
        
        if 'tables' not in config:
            raise ValueError("Missing 'tables' section in config")
        
        if not isinstance(config['tables'], list):
            raise ValueError("Section 'tables' must be a list")
        
        return config
    
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found: {config_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"YAML parse error: {e}")
    except Exception as e:
        raise Exception(f"Config read error: {e}")