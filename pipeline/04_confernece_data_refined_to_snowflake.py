# Databricks notebook source
import pyspark.sql.functions as f
from src.snowflake_credentials import *

# COMMAND ----------

df = spark.sql("show tables in conference_refined")
tables = df.select(f.collect_list('tableName')).first()[0]

# COMMAND ----------

print("Tables :")
print(*tables,sep = "\n")

# COMMAND ----------

options = snowflake_options('refined')
dbcDatabase = "conference_refined"

# COMMAND ----------

for table in tables:
    print(table,"loading......",end="")
    table_df = spark.table(f"{dbcDatabase}.{table}")
    table_df.write \
    .format("snowflake") \
    .options(**options) \
    .option("dbtable", table) \
    .mode('overwrite') \
    .save()
    print("Done")

# COMMAND ----------


