import os
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

dbc_env = os.getenv("dbc_environment")

if dbc_env == "dev" or dbc_env is None:
    sfDatabase = "ANTSTACK_DEMO_DATABASE_DEV"
    sfUser = dbutils.secrets.get("snowflake-secrets", "user-name")
    sfPassword = dbutils.secrets.get("snowflake-secrets", "password")  
    sfRole = "ANTSTACK_DEMO_ROLE_DEV"
    sfWarehouse = "ANTSTACK_DEMO_DEV"
else:
    sfDatabase = "ANTSTACK_DEMO_DATABASE_PROD"
    sfUser = dbutils.secrets.get("snowflake-secrets", "user-name-prod")
    sfPassword = dbutils.secrets.get("snowflake-secrets", "password-prod")  
    sfRole = "ANTSTACK_DEMO_ROLE_PROD"
    sfWarehouse = "ANTSTACK_DEMO_PROD"

def snowflake_options(env):
    schema = "CORE" if env == 'raw' else "CONFORMED"
    options = {
    "sfUrl": "https://jgklkfv-hyb33129.snowflakecomputing.com",
    "sfUser": sfUser,
    "sfPassword": sfPassword,
    "sfDatabase": sfDatabase,
    "sfRole" : sfRole,
    "sfSchema": schema,
    "sfWarehouse": sfWarehouse
    }
    return options