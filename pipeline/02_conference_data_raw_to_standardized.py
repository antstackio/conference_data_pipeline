# Databricks notebook source
from src.main import convert_date_object
from pyspark.sql.functions import (
    lit,
    current_timestamp,
    input_file_name,
    from_utc_timestamp,
    to_date,
    when,
    DataFrame,
    current_date,
    col,
    cast,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pull Raw layer data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Event Table

# COMMAND ----------

def read_data_from_raw(schema:str, table_name: str) -> DataFrame:
    df = spark.read.table(f"{schema}.{table_name}")
    df = df.filter(df.is_processed == 'false')
    df.printSchema()
    return df

# COMMAND ----------

event_df = read_data_from_raw('conference_raw', 'event')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Session

# COMMAND ----------

session_df = read_data_from_raw('conference_raw', 'session')

# COMMAND ----------

# MAGIC %md
# MAGIC #### In person attendee

# COMMAND ----------

inperson_attendee_df = read_data_from_raw('conference_raw', 'in_person_attendee')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Virtual attendee

# COMMAND ----------

virtual_attendee_df = read_data_from_raw('conference_raw', 'virtual_attendee')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Poll question

# COMMAND ----------

polling_questions_df = read_data_from_raw('conference_raw', 'polling_questions')

# COMMAND ----------


