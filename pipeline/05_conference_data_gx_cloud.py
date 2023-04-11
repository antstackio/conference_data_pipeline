# Databricks notebook source
# MAGIC %pip install great_expectations
# MAGIC %pip install sqlalchemy
# MAGIC %pip install snowflake-connector-python
# MAGIC %pip install snowflake-sqlalchemy

# COMMAND ----------

import os
import great_expectations as gx
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from pyspark.dbutils import DBUtils

dbutils = DBUtils()

# COMMAND ----------

dbc_env = os.getenv('dbc_environment')

# COMMAND ----------

os.environ["GX_CLOUD_ORGANIZATION_ID"] = dbutils.secrets.get("gx-cloud-secrets", "org-id")
os.environ["GX_CLOUD_ACCESS_TOKEN"] = dbutils.secrets.get("gx-cloud-secrets", "user-token")


# COMMAND ----------

context = gx.get_context()

# COMMAND ----------

if dbc_env == "dev" or dbc_env is None:
    username = dbutils.secrets.get("sf-conference-secrets", "username-dev")
    password = dbutils.secrets.get("sf-conference-secrets", "password-dev")
    role = "PROGRAMMATIC_ACCESS_ROLE_DEV"
    warehouse = "DEV"
    snowflake_connection = f"""snowflake://{username}:{password}@kwtqhde-zi48131/CONFERENCE_DEV/CONFORMED?warehouse={warehouse}&role={role}&application=great_expectations_cloud"""
elif dbc_env == "stage":
    username = dbutils.secrets.get("sf-conference-secrets", "username-stage")
    password = dbutils.secrets.get("sf-conference-secrets", "password-stage")
    role = "PROGRAMMATIC_ACCESS_ROLE_STAGE"
    warehouse = "STAGE"
    snowflake_connection = f"""snowflake://{username}:{password}@kwtqhde-zi48131/CONFERENCE_STAGE/CONFORMED?warehouse={warehouse}&role={role}&application=great_expectations_cloud"""
else:
    pass

# COMMAND ----------

print(snowflake_connection)

# COMMAND ----------

print(context)

# COMMAND ----------

# Create a Datasource
    
# Define a Datasource YAML Config:
# by modifying the following YAML configuration

datasource_yaml = f"""
name: snowflake
class_name: Datasource
execution_engine:
    class_name: SqlAlchemyExecutionEngine
    connection_string: {snowflake_connection}
data_connectors:
    default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - attendee_dim_batch
    default_inferred_data_connector_name:
        class_name: InferredAssetSqlDataConnector
        include_schema_name: true
"""

# Test your configuration (Optional):
datasource = context.test_yaml_config(datasource_yaml)

# Save your datasource:
datasource = context.save_datasource(datasource)

# Confirm the datasource has been saved (Optional):
existing_datasource = context.get_datasource(datasource_name=datasource.name)
print(existing_datasource.config)

# COMMAND ----------

expectation_suite = context.create_expectation_suite(
    expectation_suite_name="conference_attendee_dim_ex_suite"
)


# COMMAND ----------

expectation_configuration = gx.core.ExpectationConfiguration(**{
    "expectation_type": "expect_column_values_to_not_be_null",
    "kwargs": {
      "column": 'state',
    },
    "meta":{}
  })
expectation_suite.add_expectation(expectation_configuration)


# COMMAND ----------

expectation_configuration = gx.core.ExpectationConfiguration(**{
    "expectation_type": "expect_column_values_to_not_be_null",
    "kwargs": {
      "column": 'email_address',
    },
    "meta":{}
  })
expectation_suite.add_expectation(expectation_configuration)


context.save_expectation_suite(expectation_suite)

# COMMAND ----------



# COMMAND ----------

run_time_batch_request = RuntimeBatchRequest(
    datasource_name="snowflake",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="conference.event_registrant.asset",  # this can be anything that identifies this data
    runtime_parameters={
        "query": f"SELECT * from CONFORMED.EVENT_REGISTRANT_DIM"
    },
    batch_identifiers={"attendee_dim_batch": "attendee_dim_batch"},
)

# COMMAND ----------

# First, fetch the ExpectationSuite you will use to define a Validation
expectation_suite = context.get_expectation_suite(
    expectation_suite_name="conference_attendee_dim_ex_suite"
)  # add your expectation suite name here

checkpoint_name = "new_checkpoint"  # name your checkpoint here!

# uncomment the lines below after successfully creating your Checkpoint to run this code again!
# checkpoint = context.get_checkpoint(checkpoint_name)
# checkpoint_id = checkpoint.ge_cloud_id

checkpoint_config = {
    "name": checkpoint_name,
    "validations": [
        {
            "expectation_suite_name": expectation_suite.expectation_suite_name,
            "expectation_suite_ge_cloud_id": expectation_suite.ge_cloud_id,
            "batch_request": run_time_batch_request,
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "store_evaluation_params",
                    "action": {"class_name": "StoreEvaluationParametersAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction"},
                },
            ],
        }
    ],
    "config_version": 1,
    "class_name": "Checkpoint",
}

context.add_or_update_checkpoint(**checkpoint_config)
checkpoint = context.get_checkpoint(checkpoint_name)
print(checkpoint)

# COMMAND ----------

# checkpoint = context.get_checkpoint(checkpoint_name)
checkpoint_id = checkpoint.ge_cloud_id
print(checkpoint_id)

# COMMAND ----------

# Optionally run the Checkpoint:
result = context.run_checkpoint(checkpoint_name=checkpoint_name)

# COMMAND ----------


