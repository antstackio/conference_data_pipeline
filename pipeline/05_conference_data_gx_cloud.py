# Databricks notebook source
# MAGIC %pip install great_expectations
# MAGIC %pip install sqlalchemy
# MAGIC %pip install snowflake-connector-python
# MAGIC %pip install snowflake-sqlalchemy

# COMMAND ----------

import os
import great_expectations as gx
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.core import ExpectationConfiguration
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

# Create a Datasource
    
# Define a Datasource YAML Config:
# by modifying the following YAML configuration

datasource_yaml = f"""
name: snowflake_dev
class_name: Datasource
execution_engine:
    class_name: SqlAlchemyExecutionEngine
    connection_string: {snowflake_connection}
data_connectors:
    default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - event_dim_batch
            - session_dim_batch
            - event_registrant_dim_batch
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

# MAGIC %md
# MAGIC #### Event Expectation Suite

# COMMAND ----------

event_expectation_suite = context.create_expectation_suite(
    expectation_suite_name="conference_event_dim_ex_suite_dev"
)


# COMMAND ----------

expectation_configuration = ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={
      "column": 'event_name',
    }
  )
event_expectation_suite.add_expectation(expectation_configuration)


# COMMAND ----------

expectation_configuration = ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={
      "column": 'start_date',
    }
  )
event_expectation_suite.add_expectation(expectation_configuration)

# COMMAND ----------

expectation_configuration = ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={
      "column": 'end_date',
    }
  )
event_expectation_suite.add_expectation(expectation_configuration)

# COMMAND ----------

expectation_configuration = ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={
      "column": 'zip_code',
    }
  )
event_expectation_suite.add_expectation(expectation_configuration)

# COMMAND ----------

expectation_configuration = ExpectationConfiguration(**{
    "expectation_type": "expect_column_values_to_be_in_set",
    "kwargs": {
      "column": 'country',
      "value_set": ['us', 'can']
    },
    "meta":{}
  })
event_expectation_suite.add_expectation(expectation_configuration)


context.save_expectation_suite(event_expectation_suite)

# COMMAND ----------

event_batch_request = RuntimeBatchRequest(
    datasource_name="snowflake_dev",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="conference.event_dim.asset",  # this can be anything that identifies this data
    runtime_parameters={
        "query": f"SELECT * from CONFORMED.EVENT_DIM"
    },
    batch_identifiers={"event_dim_batch": "event_dim_batch"},
)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Session Expectation Suite

# COMMAND ----------

session_expectation_suite = context.create_expectation_suite(
    expectation_suite_name="conference_session_dim_ex_suite_dev"
)

# COMMAND ----------

expectation_configuration = ExpectationConfiguration(**{
    "expectation_type": "expect_column_values_to_not_be_null",
    "kwargs": {
      "column": 'session_title',
    },
    "meta":{}
  })
session_expectation_suite.add_expectation(expectation_configuration)

# COMMAND ----------

expectation_configuration = ExpectationConfiguration(**{
    "expectation_type": "expect_column_values_to_not_be_null",
    "kwargs": {
      "column": 'start_time',
    },
    "meta":{}
  })
session_expectation_suite.add_expectation(expectation_configuration)

# COMMAND ----------

expectation_configuration = ExpectationConfiguration(**{
    "expectation_type": "expect_column_values_to_not_be_null",
    "kwargs": {
      "column": 'end_time',
    },
    "meta":{}
  })
session_expectation_suite.add_expectation(expectation_configuration)

# COMMAND ----------

context.save_expectation_suite(session_expectation_suite)

# COMMAND ----------

session_batch_request = RuntimeBatchRequest(
    datasource_name="snowflake_dev",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="conference.session_dim.asset",  # this can be anything that identifies this data
    runtime_parameters={
        "query": f"SELECT * from CONFORMED.SESSION_DIM"
    },
    batch_identifiers={"session_dim_batch": "session_dim_batch"},
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Event Registrants Suite

# COMMAND ----------

event_registrants_expectation_suite = context.create_expectation_suite(
    expectation_suite_name="conference_event_registrants_dim_ex_suite_dev"
)


# COMMAND ----------

expectation_configuration = ExpectationConfiguration(**{
    "expectation_type": "expect_column_values_to_not_be_null",
    "kwargs": {
      "column": 'email_address',
    },
    "meta":{}
  })
event_registrants_expectation_suite.add_expectation(expectation_configuration)

# COMMAND ----------

expectation_configuration = ExpectationConfiguration(**{
    "expectation_type": "expect_column_values_to_not_be_null",
    "kwargs": {
      "column": 'registration_no',
    },
    "meta":{}
  })
event_registrants_expectation_suite.add_expectation(expectation_configuration)

# COMMAND ----------

expectation_configuration = ExpectationConfiguration(**{
    "expectation_type": "expect_column_values_to_not_be_null",
    "kwargs": {
      "column": 'event_name',
    },
    "meta":{}
  })
event_registrants_expectation_suite.add_expectation(expectation_configuration)

# COMMAND ----------

expectation_configuration = ExpectationConfiguration(**{
    "expectation_type": "expect_column_values_to_not_be_null",
    "kwargs": {
      "column": 'event_id',
    },
    "meta":{}
  })
event_registrants_expectation_suite.add_expectation(expectation_configuration)

# COMMAND ----------

expectation_configuration = ExpectationConfiguration(**{
    "expectation_type": "expect_column_values_to_not_be_null",
    "kwargs": {
      "column": 'state',
    },
    "meta":{}
  })
event_registrants_expectation_suite.add_expectation(expectation_configuration)

# COMMAND ----------

context.save_expectation_suite(event_registrants_expectation_suite)

# COMMAND ----------

event_registrant_batch_request = RuntimeBatchRequest(
    datasource_name="snowflake_dev",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="conference.event_registrant_dim.asset",  # this can be anything that identifies this data
    runtime_parameters={
        "query": f"SELECT * from CONFORMED.EVENT_REGISTRANT_DIM"
    },
    batch_identifiers={"event_registrant_dim_batch": "event_registrant_dim_batch"},
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checkpoint Definition

# COMMAND ----------

# First, fetch the ExpectationSuite you will use to define a Validation
event_expectation_suite = context.get_expectation_suite(
    expectation_suite_name="conference_event_dim_ex_suite_dev"
)  # add your expectation suite name here

session_expectation_suite = context.get_expectation_suite(
    expectation_suite_name="conference_session_dim_ex_suite_dev"
)

event_registrant_expectation_suite = context.get_expectation_suite(
    expectation_suite_name="conference_event_registrants_dim_ex_suite_dev"
)


checkpoint_name = "conference_data_checkpoint_v1"  # name your checkpoint here!

# uncomment the lines below after successfully creating your Checkpoint to run this code again!
# checkpoint = context.get_checkpoint(checkpoint_name)
# checkpoint_id = checkpoint.ge_cloud_id

checkpoint_config = {
    "name": checkpoint_name,
    "validations": [
        {
            "expectation_suite_name": event_expectation_suite.expectation_suite_name,
            "expectation_suite_ge_cloud_id": event_expectation_suite.ge_cloud_id,
            "batch_request": event_batch_request,
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
        },
        {
            "expectation_suite_name": session_expectation_suite.expectation_suite_name,
            "expectation_suite_ge_cloud_id": session_expectation_suite.ge_cloud_id,
            "batch_request": session_batch_request,
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
        },
        {
            "expectation_suite_name": event_registrant_expectation_suite.expectation_suite_name,
            "expectation_suite_ge_cloud_id": event_registrant_expectation_suite.ge_cloud_id,
            "batch_request": event_registrant_batch_request,
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


