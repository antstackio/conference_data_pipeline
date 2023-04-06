# Databricks notebook source
spark

# COMMAND ----------

import os

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

mvCommand = f'aws s3 mv s3://{bucket}processed/ s3://{src_file_path} --recursive --acl bucket-owner-full-control'
print(mvCommand)
# resp = os.system(mvCommand)
