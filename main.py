import yaml
from getTableSchema import getTableSchema

# Load configDB.yaml
with open("configDB.yaml", "r") as f:
    config = yaml.safe_load(f)

# Gọi hàm
columns = getTableSchema(config)

# In kết quả
for column in columns:
    print(column)
