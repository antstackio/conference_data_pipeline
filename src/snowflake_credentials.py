from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

sfDatabase = "ANTSTACK_DEMO_DATABASE_DEV"
sfUser = dbutils.secrets.get("snowflake-secrets", "user-name")
sfPassword = dbutils.secrets.get("snowflake-secrets", "password")  
sfRole = "ANTSTACK_DEMO_ROLE_DEV"
sfWarehouse = "ANTSTACK_DEMO_DEV"

def snowflake_options(env):
    schema = "CORE" if env == 'raw' else "CONFORMED"
    options = {
    "sfUrl": "https://jgklkfv-hyb33129.snowflakecomputing.com",
    "sfUser": sfUser,
    "sfPassword": sfPassword,
    "sfDatabase": sfDatabase,
    "sfRole" : sfRole,
    "sfSchema": schema,
    "sfWarehouse": sfWarehouse,
    "truncate_table": "on"
    }
    return options