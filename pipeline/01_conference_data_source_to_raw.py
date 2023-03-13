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

session_df = read_file_from_src_path(spark, src_file_path, SESSION, session_schema)

# COMMAND ----------

inperson_attendee_df = read_file_from_src_path(spark, src_file_path, INPERSONATTENDEE, attendee_schema)

# COMMAND ----------

virtual_attendee_df = read_file_from_src_path(spark, src_file_path, VIRTUALATTENDEE, attendee_schema)

# COMMAND ----------

poll_question_df = read_file_from_src_path(spark, src_file_path, POLLQUESTIONS, poll_question_schema)

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


