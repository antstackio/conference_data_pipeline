# Databricks notebook source
from src.main import convert_date_object
from pyspark.sql.types import StringType
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

def add_attendee_type(df: DataFrame, attendee_type: str) -> DataFrame:
    result_df = df.withColumn('attendee_type', lit(attendee_type))
    return result_df

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

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Event

# COMMAND ----------

event_df = convert_date_object(event_df, "start_date", "dd/MM/y")
display(event_df)

# COMMAND ----------

event_df = convert_date_object(event_df, "end_date", "dd/MM/y")
display(event_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Session

# COMMAND ----------

session_df = convert_date_object(session_df, "session_date", "d/M/y")
display(session_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Attendee

# COMMAND ----------

inperson_attendee_df = add_attendee_type(inperson_attendee_df, 'inperson')

# COMMAND ----------

inperson_attendee_df = inperson_attendee_df.withColumn('login_time', lit(None).cast(StringType()))

# COMMAND ----------

inperson_attendee_df = inperson_attendee_df.withColumn('logout_time', lit(None).cast(StringType()))

# COMMAND ----------

attendee_columns = ['registration_no', 'first_name', 'last_name', 'email_address', 'job_role', 'state', 'session_title', 'attendee_type', 'login_time', 'logout_time']
inperson_attendee_df = inperson_attendee_df.select(*attendee_columns)
inperson_attendee_df.printSchema()

# COMMAND ----------

inperson_attendee_df = inperson_attendee_df.select(*attendee_columns)
inperson_attendee_df.printSchema()

# COMMAND ----------

virtual_attendee_df = add_attendee_type(virtual_attendee_df, 'virtual')

# COMMAND ----------

virtual_attendee_df = virtual_attendee_df.select(*attendee_columns)
virtual_attendee_df.printSchema()

# COMMAND ----------

print('Inperson attendee count', inperson_attendee_df.count(), sep=' :: ')
print('Virtual attendee count', virtual_attendee_df.count(), sep=' :: ')

# COMMAND ----------

attendee = inperson_attendee_df.union(virtual_attendee_df)

# COMMAND ----------

attendee.printSchema()

# COMMAND ----------

print('Total attendee count', attendee.count(), sep=' :: ')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load

# COMMAND ----------

import os

# COMMAND ----------

os.listdir('../SqlDBM/src/Tables/')

# COMMAND ----------

with open('../SqlDBM/src/Tables/conference_refined.event.sql') as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

with open('../SqlDBM/src/Tables/conference_refined.session.sql') as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

with open('../SqlDBM/src/Tables/conference_refined.registrant.sql') as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

with open('../SqlDBM/src/Tables/conference_refined.polling_questions.sql') as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------


