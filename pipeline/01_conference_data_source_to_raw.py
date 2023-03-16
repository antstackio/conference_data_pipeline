# Databricks notebook source
from src.main import read_file_from_src_path, add_required_columns
from src.schema import *
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

failed_to_load: list = []
failed_to_add: list = []

# COMMAND ----------

try:
    event_df = read_file_from_src_path(spark, src_file_path, EVENT, schema=event_schema)
except Exception as e:
    print(e)
    failed_to_load.append(True)

# COMMAND ----------

display(event_df)

# COMMAND ----------

try:
    session_df = read_file_from_src_path(spark, src_file_path, SESSION, session_schema)
except Exception as e:
    print(e)
    failed_to_load.append(True)

# COMMAND ----------

display(session_df)

# COMMAND ----------

try:
    inperson_attendee_df = read_file_from_src_path(spark, src_file_path, INPERSONATTENDEE, attendee_schema)
except Exception as e:
    print(e)
    failed_to_load.append(True)

# COMMAND ----------

display(inperson_attendee_df)

# COMMAND ----------

try:
    virtual_attendee_df = read_file_from_src_path(spark, src_file_path, VIRTUALATTENDEE, attendee_schema)
except Exception as e:
    print(e)
    failed_to_load.append(True)

# COMMAND ----------

display(virtual_attendee_df)

# COMMAND ----------

try:
    poll_questions_df = read_file_from_src_path(spark, src_file_path, POLLQUESTIONS, poll_question_schema)
except Exception as e:
    print(e)
    failed_to_load.append(True)

# COMMAND ----------

if any(failed_to_load):
  dbutils.jobs.taskValues.set(key="execute_refined_layer", value=False)
  dbutils.notebook.exit("Failed to load one or more input files exiting the notebook!")
else:
  dbutils.jobs.taskValues.set(key="execute_refined_layer", value=True)

# COMMAND ----------

display(poll_questions_df.head(5))

# COMMAND ----------

current_user = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .userName()
    .get()
)

# COMMAND ----------

try:
    event_target_df = add_required_columns(event_df, current_user)
except Exception as e:
    print(e)
    failed_to_add.append(True)

# COMMAND ----------

try:
    session_target_df = add_required_columns(session_df, current_user)
except Exception as e:
    print(e)
    failed_to_add.append(True)

# COMMAND ----------

try:
    inperson_attendee_target_df = add_required_columns(inperson_attendee_df, current_user)
except Exception as e:
    print(e)
    failed_to_add.append(True)

# COMMAND ----------

try:
    virtual_attendee_target_df = add_required_columns(virtual_attendee_df, current_user)
except Exception as e:
    print(e)
    failed_to_add.append(True)

# COMMAND ----------

try:
    questions_target_df = add_required_columns(poll_questions_df, current_user)
except Exception as e:
    print(e)
    failed_to_add.append(True)

# COMMAND ----------

if any(failed_to_add):
    dbutils.jobs.taskValues.set(key="execute_refined_layer", value=False)
    dbutils.notebook.exit("Failed to load one or more input files exiting the notebook!")
else:
  dbutils.jobs.taskValues.set(key="execute_refined_layer", value=True)

# COMMAND ----------

event_target_df.write.format("delta").mode(MODE).saveAsTable(f"conference_raw.event")

# COMMAND ----------

session_target_df.write.format("delta").mode(MODE).saveAsTable(f"conference_raw.session")

# COMMAND ----------

inperson_attendee_target_df.write.format("delta").mode(MODE).saveAsTable(
    f"conference_raw.in_person_attendee"
)

# COMMAND ----------

virtual_attendee_target_df.write.format("delta").mode(MODE).saveAsTable(
    f"conference_raw.virtual_attendee"
)

# COMMAND ----------

questions_target_df.write.format("delta").mode(MODE).saveAsTable(
    f"conference_raw.polling_questions"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM conference_raw.event;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM conference_raw.session;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM conference_raw.in_person_attendee;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM conference_raw.virtual_attendee;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM conference_raw.polling_questions;

# COMMAND ----------


