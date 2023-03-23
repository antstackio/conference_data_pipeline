# Databricks notebook source
from src.main import (
    convert_date_object,
    read_data_from_raw,
    add_attendee_type,
    create_timestamp_str,
    select_columns_from_dataframe,
    union_dataframes,
)
from pyspark.sql import SparkSession
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
    concat,
    to_timestamp,
)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pull Raw layer data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Event Table

# COMMAND ----------

event_df = read_data_from_raw(spark, "conference_raw", "event")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Session

# COMMAND ----------

session_df = read_data_from_raw(spark, "conference_raw", "session")

# COMMAND ----------

# MAGIC %md
# MAGIC #### In person attendee

# COMMAND ----------

inperson_attendee_df = read_data_from_raw(spark, "conference_raw", "in_person_attendee")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Virtual attendee

# COMMAND ----------

virtual_attendee_df = read_data_from_raw(spark, "conference_raw", "virtual_attendee")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Poll question

# COMMAND ----------

polling_questions_df = read_data_from_raw(spark, "conference_raw", "polling_questions")

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

inperson_attendee_df = add_attendee_type(inperson_attendee_df, "inperson")

# COMMAND ----------

inperson_attendee_df = inperson_attendee_df.withColumn(
    "login_time", lit(None).cast(StringType())
)

# COMMAND ----------

inperson_attendee_df = inperson_attendee_df.withColumn(
    "logout_time", lit(None).cast(StringType())
)

# COMMAND ----------

virtual_registants_df = virtual_attendee_df.filter(
    virtual_attendee_df.login_time.isNull() & virtual_attendee_df.logout_time.isNull()
)

# COMMAND ----------

virtual_registants_df.printSchema()

# COMMAND ----------

print(virtual_registants_df.count())

# COMMAND ----------

virtual_attendee_df = virtual_attendee_df.filter(
    virtual_attendee_df.login_time.isNotNull()
    & virtual_attendee_df.logout_time.isNotNull()
).withColumnRenamed("session_title", "session")

# COMMAND ----------

virtual_attendee_df.printSchema()

# COMMAND ----------

virtual_attendee_df = virtual_attendee_df.join(
    session_df,
    virtual_attendee_df.session == session_df.session_title,
    "left",
).select(
    virtual_attendee_df.registration_no,
    virtual_attendee_df.first_name,
    virtual_attendee_df.last_name,
    virtual_attendee_df.email_address,
    virtual_attendee_df.job_role,
    virtual_attendee_df.state,
    virtual_attendee_df.session,
    virtual_attendee_df.login_time,
    virtual_attendee_df.logout_time,
    session_df.session_date,
)

# COMMAND ----------

virtual_attendee_df = create_timestamp_str(virtual_attendee_df, 'session_date', 'login_time', 'login_time_')
display(virtual_attendee_df.head(5))

# COMMAND ----------

virtual_attendee_df = create_timestamp_str(virtual_attendee_df, 'session_date', 'logout_time', 'logout_time_')
display(virtual_attendee_df.head(5))

# COMMAND ----------

v_df = virtual_attendee_df.withColumn(
    "login_time_", to_timestamp(virtual_attendee_df.login_time_)
).withColumn("logout_time_", to_timestamp(virtual_attendee_df.logout_time_))

display(v_df)

# COMMAND ----------

virtual_attendee_df = v_df.drop("login_time", "logout_time")
virtual_attendee_df.printSchema()

# COMMAND ----------

virtual_attendee_df = (
    virtual_attendee_df.withColumnRenamed("login_time_", "login_time")
    .withColumnRenamed("logout_time_", "logout_time")
    .withColumnRenamed("session", "session_title")
)
virtual_attendee_df.printSchema()

# COMMAND ----------

attendee_columns = [
    "registration_no",
    "first_name",
    "last_name",
    "email_address",
    "job_role",
    "state",
    "session_title",
    "attendee_type",
    "login_time",
    "logout_time",
]

inperson_attendee_df = select_columns_from_dataframe(inperson_attendee_df, attendee_columns)

# COMMAND ----------

print('In person attendee count', inperson_attendee_df.count(), sep=" :: ")
print('virtual attendee count', virtual_attendee_df.count(), sep=" :: ")
print('virtual registrants count', virtual_registants_df.count(), sep=" :: ")

# COMMAND ----------

virtual_registants_df = add_attendee_type(virtual_registants_df, 'virtual')

virtual_registants_df = select_columns_from_dataframe(virtual_registants_df, attendee_columns)

# COMMAND ----------

virtual_attendee_df = add_attendee_type(virtual_attendee_df, "virtual")

# COMMAND ----------

virtual_attendee_df = select_columns_from_dataframe(virtual_attendee_df, attendee_columns)

# COMMAND ----------

virtual_attendee_df = union_dataframes(virtual_attendee_df, virtual_registants_df)

# COMMAND ----------

print("Inperson attendee count", inperson_attendee_df.count(), sep=" :: ")
print("Virtual attendee count", virtual_attendee_df.count(), sep=" :: ")

# COMMAND ----------

attendee = union_dataframes(inperson_attendee_df, virtual_attendee_df)

# COMMAND ----------

print("Total attendee count", attendee.count(), sep=" :: ")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load

# COMMAND ----------

import os

# COMMAND ----------

os.listdir("../SqlDBM/src/Tables/")

# COMMAND ----------

with open("../SqlDBM/src/Tables/conference_refined.event.sql") as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conference_refined.session.sql") as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conference_refined.registrant.sql") as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conference_refined.polling_questions.sql") as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------


