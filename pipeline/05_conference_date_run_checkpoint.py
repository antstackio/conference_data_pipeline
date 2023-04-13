# Databricks notebook source
# MAGIC %pip install sqlalchemy
# MAGIC %pip install snowflake-connector-python
# MAGIC %pip install snowflake-sqlalchemy

# COMMAND ----------

import os
import great_expectations as gx
from pyspark.dbutils import DBUtils

dbutils = DBUtils()
os.environ["GX_CLOUD_BASE_URL"] = "https://api.greatexpectations.io"

# COMMAND ----------

os.environ["GX_CLOUD_ORGANIZATION_ID"] = dbutils.secrets.get("gx-cloud-secrets", "org-id")
os.environ["GX_CLOUD_ACCESS_TOKEN"] = dbutils.secrets.get("gx-cloud-secrets", "user-token")

# COMMAND ----------

context = gx.get_context()

# COMMAND ----------

dbc_env = os.getenv('dbc_environment')

# COMMAND ----------

if dbc_env == 'dev' or dbc_env is None:
    checkpoint = context.get_checkpoint(name='conference_data_checkpoint_v1')
elif dbc_env == 'stage':
    checkpoint = context.get_checkpoint(name='conference_data_checkpoint_stage_v1')
else:
    checkpoint = context.get_checkpoint(name='conference_data_checkpoint_prod_v1') # Doesn't exist


# COMMAND ----------

context.run_checkpoint(ge_cloud_id=checkpoint.ge_cloud_id)

# COMMAND ----------


