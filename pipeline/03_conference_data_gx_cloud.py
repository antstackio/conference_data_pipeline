# Databricks notebook source
import os
import great_expectations as gx

os.environ["GX_CLOUD_BASE_URL"] = "https://api.greatexpectations.io"

# COMMAND ----------

context = gx.get_context()

# COMMAND ----------

attendee_dim_df = spark.read.table('conference_refined.event_registrant_dim')
attendee_dim_df = attendee_dim_df.filter(attendee_dim_df.create_date == '2023-03-17')

# COMMAND ----------

datasource_yaml = """
name: conference_refined_attendee_dim
class_name: Datasource
execution_engine:
    class_name: SparkDFExecutionEngine
data_connectors:
    default_runtime_data_connector:
        class_name: RuntimeDataConnector
        batch_identifiers: 
            - attendee_dim_batch_idf
"""

# Test your configuration (Optional):
datasource: Datasource = context.test_yaml_config(datasource_yaml)

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

checkpoint_name = "conference_attendee_dim_ex_checkpoint"
config_version = 1
class_name = "Checkpoint"

# First, fetch the ExpectationSuite we will use to define a Validation
expectation_suite = context.get_expectation_suite(expectation_suite_name="conference_attendee_dim_ex_suite")

validations = [{
    "expectation_suite_name": expectation_suite.name,
    "expectation_suite_ge_cloud_id": expectation_suite.ge_cloud_id,
    "batch_request": {
        "datasource_name": "conference_refined_attendee_dim",
        "data_connector_name": "default_runtime_data_connector",
        "data_asset_name": "conference_attende_dim_data_asset",
    },
}]

# NOTE: To update an existing Checkpoint, you must include the Checkpoint's ge_cloud_id as a named argument.
checkpoint = context.add_or_update_checkpoint(
    name=checkpoint_name,
    config_version=config_version,
    class_name=class_name,
    validations=validations,
)

# Confirm the Checkpoint has been saved:
checkpoint_id = str(checkpoint.ge_cloud_id)
checkpoint = context.get_checkpoint(ge_cloud_id=checkpoint_id)
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

