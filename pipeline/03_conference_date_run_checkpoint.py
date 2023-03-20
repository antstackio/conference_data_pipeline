# Databricks notebook source
import os
import great_expectations as gx
from pyspark.sql.functions import current_date

os.environ["GX_CLOUD_BASE_URL"] = "https://api.greatexpectations.io"

# COMMAND ----------

context = gx.get_context()

# COMMAND ----------

attendee_dim_df = spark.read.table('conference_refined.event_registrant_dim')
attendee_dim_df = attendee_dim_df.filter(attendee_dim_df.create_date == current_date())

# COMMAND ----------

checkpoint = context.get_checkpoint(name='conference_attendee_dim_ex_checkpoint')
print(checkpoint)

# COMMAND ----------

batch_request = {
    "runtime_parameters": {
        "batch_data": attendee_dim_df
    },
    "batch_identifiers": {
        "attendee_dim_batch_idf": "initial_data"
    }
}
context.run_checkpoint(ge_cloud_id=checkpoint.ge_cloud_id, batch_request=batch_request)

# COMMAND ----------


