# Databricks notebook source
from src.main import read_file_from_src_path, add_required_columns
from src.schema import *
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

event_df = read_file_from_src_path(spark, src_file_path, EVENT, schema=event_schema)

# COMMAND ----------

display(event_df)

# COMMAND ----------

session_df = read_file_from_src_path(spark, src_file_path, SESSION, session_schema)

# COMMAND ----------

display(session_df)

# COMMAND ----------

inperson_attendee_df = read_file_from_src_path(spark, src_file_path, INPERSONATTENDEE, attendee_schema)

# COMMAND ----------

display(inperson_attendee_df)

# COMMAND ----------

virtual_attendee_df = read_file_from_src_path(spark, src_file_path, VIRTUALATTENDEE, attendee_schema)

# COMMAND ----------

display(virtual_attendee_df)

# COMMAND ----------

poll_questions_df = read_file_from_src_path(spark, src_file_path, POLLQUESTIONS, poll_question_schema)

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

event_target_df = add_required_columns(event_df, current_user)

# COMMAND ----------

session_target_df = add_required_columns(session_df, current_user)

# COMMAND ----------

inperson_attendee_target_df = add_required_columns(inperson_attendee_df, current_user)

# COMMAND ----------

virtual_attendee_target_df = add_required_columns(virtual_attendee_df, current_user)

# COMMAND ----------

questions_target_df = add_required_columns(poll_questions_df, current_user)

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
# MAGIC SELECT * FROM conference_raw.event;

# COMMAND ----------


