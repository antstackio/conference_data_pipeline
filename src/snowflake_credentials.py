import os
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

dbc_env = os.getenv("dbc_environment")

if dbc_env == "dev" or dbc_env is None:
    sfDatabase = "CONFERENCE_DEV"
    sfUser = dbutils.secrets.get("sf-conference-secrets", "username-dev")
    sfPassword = dbutils.secrets.get("sf-conference-secrets", "password-dev")  
    sfRole = "PROGRAMMATIC_ACCESS_ROLE_DEV"
    sfWarehouse = "DEV"
elif dbc_env == 'stage':
    sfDatabase = "CONFERENCE_STAGE"
    sfUser = dbutils.secrets.get("sf-conference-secrets", "username-stage")
    sfPassword = dbutils.secrets.get("sf-conference-secrets", "password-stage")  
    sfRole = "PROGRAMMATIC_ACCESS_ROLE_STAGE"
    sfWarehouse = "STAGE"
else:
    sfDatabase = "CONFERENCE_PROD"
    sfUser = dbutils.secrets.get("sf-conference-secrets", "username-prod")
    sfPassword = dbutils.secrets.get("sf-conference-secrets", "password-prod")  
    sfRole = "PROGRAMMATIC_ACCESS_ROLE_PROD"
    sfWarehouse = "PROD"

def snowflake_options(env):
    schema = "CORE" if env == 'raw' else "CONFORMED"
    options = {
    "sfUrl": "https://kwtqhde-zi48131.snowflakecomputing.com",
    "sfUser": sfUser,
    "sfPassword": sfPassword,
    "sfDatabase": sfDatabase,
    "sfRole" : sfRole,
    "sfSchema": schema,
    "sfWarehouse": sfWarehouse
    }
    return options
