# Databricks notebook source
import os

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Clean Up

# COMMAND ----------

# MAGIC %md
# MAGIC ### Move files back to the input location

# COMMAND ----------

dbc_env = os.getenv("dbc_environment")

if dbc_env == "dev" or dbc_env is None:
    # dev src file path
    bucket = "conference-data-ap-south-1-landing-dev/"
    src_file_path = bucket + 'input/'
elif dbc_env == 'stage':
    bucket = "conference-data-ap-south-1-landing-stage/"
    src_file_path = bucket + 'input/'
else:
    # Production src file
    bucket = "conference-data-ap-south-1-landing-prod/"
    src_file_path = bucket + 'input/'


# COMMAND ----------

try:
    mvCommand = f'aws s3 mv s3://{bucket}processed/ s3://{src_file_path} --recursive --acl bucket-owner-full-control'
    print(mvCommand)
    resp = os.system(mvCommand)
    print(resp)
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Delete the databases and tables

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE conference_raw CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE conference_refined CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE conference_trusted CASCADE
