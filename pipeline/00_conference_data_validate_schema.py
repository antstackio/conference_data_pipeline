# Databricks notebook source
from src.schema import *
from src.main import file_exists, compare_schema, convert_schema_to_list
from pyspark.sql import SparkSession

# COMMAND ----------

file_exists_check_list: list = []
schema_validation_check_list: list = []

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

event_file_exists_result = file_exists(spark, src_file_path, EVENT)

display(event_file_exists_result)

event_file_exists = event_file_exists_result["file_exists"]
event_inferred_schema = event_file_exists_result["schema"]
event_filename = event_file_exists_result["filename"]

file_exists_check_list.append(event_file_exists_result)

# COMMAND ----------

if event_file_exists:
    event_defined_schema = convert_schema_to_list(event_schema)
    print("Event Defined Schema ", event_defined_schema, sep=":: ")
    print("Event Inferred Schema ", event_inferred_schema, sep=":: ")

    event_compare_schema_result = compare_schema(
        event_inferred_schema, event_defined_schema, event_filename
    )
    print(event_compare_schema_result)
    schema_validation_check_list.append(event_compare_schema_result)

# COMMAND ----------

session_file_exists_result = file_exists(spark, src_file_path, SESSION)
session_file_exists = session_file_exists_result["file_exists"]
session_inferred_schema = session_file_exists_result["schema"]
session_filename = session_file_exists_result["filename"]

file_exists_check_list.append(session_file_exists_result)
print(session_file_exists_result)

# COMMAND ----------

if session_file_exists:
    session_defined_schema = convert_schema_to_list(session_schema)
    print("Session Defined Schema ", session_defined_schema, sep=":: ")
    print("Session Inferred Schema ", session_inferred_schema, sep=":: ")

    session_compare_schema_result = compare_schema(
        session_inferred_schema, session_defined_schema, session_filename
    )

    schema_validation_check_list.append(session_compare_schema_result)
    print(session_compare_schema_result)

# COMMAND ----------

inperson_file_exists_result = file_exists(spark, src_file_path, INPERSONATTENDEE)
inperson_file_exists = inperson_file_exists_result["file_exists"]
inperson_inferred_schema = inperson_file_exists_result["schema"]
inperson_filename = inperson_file_exists_result["filename"]

file_exists_check_list.append(inperson_file_exists_result)
print(inperson_file_exists_result)

# COMMAND ----------

if inperson_file_exists:
    inperson_defined_schema = convert_schema_to_list(attendee_schema)
    print("Inperson Defined Schema ", inperson_defined_schema, sep=":: ")
    print("Inperson Inferred Schema ", inperson_inferred_schema, sep=":: ")

    inperson_compare_schema_result = compare_schema(
        inperson_inferred_schema, inperson_defined_schema, inperson_filename
    )
    schema_validation_check_list.append(inperson_compare_schema_result)
    print(inperson_compare_schema_result)

# COMMAND ----------

virtual_file_exists_result = file_exists(spark, src_file_path, VIRTUALATTENDEE)
virtual_file_exists = virtual_file_exists_result["file_exists"]
virtual_inferred_schema = virtual_file_exists_result["schema"]
virtual_filename = virtual_file_exists_result["filename"]

file_exists_check_list.append(virtual_file_exists_result)
print(virtual_file_exists_result)

# COMMAND ----------

if virtual_file_exists:
    virtual_defined_schema = convert_schema_to_list(attendee_schema)
    print("Virtual Defined Schema ", virtual_defined_schema, sep=":: ")
    print("Virtual Inferred Schema ", virtual_inferred_schema, sep=":: ")

    virtual_compare_schema_result = compare_schema(
        virtual_inferred_schema, virtual_defined_schema, virtual_filename
    )

    schema_validation_check_list.append(virtual_compare_schema_result)
    print(virtual_compare_schema_result)

# COMMAND ----------

questions_file_exists_result = file_exists(spark, src_file_path, POLLQUESTIONS)
questions_file_exists = questions_file_exists_result["file_exists"]
questions_inferred_schema = questions_file_exists_result["schema"]
questions_filename = questions_file_exists_result["filename"]

file_exists_check_list.append(questions_file_exists_result)
print(questions_file_exists_result)

# COMMAND ----------

if questions_file_exists:
    questions_defined_schema = convert_schema_to_list(poll_question_schema)
    print("Questions Defined Schema ", questions_defined_schema, sep=":: ")
    print("Questions Inferred Schema ", questions_inferred_schema, sep=":: ")

    questions_compare_schema_result = compare_schema(
        questions_inferred_schema, questions_defined_schema, questions_filename
    )
    schema_validation_check_list.append(questions_compare_schema_result)
    print(questions_compare_schema_result)

# COMMAND ----------




# MAGIC %md