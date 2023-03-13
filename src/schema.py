import os
from pyspark.dbutils import DBUtils
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    DateType,
    TimestampType,
)

dbc_env = os.getenv("dbc_environment")

if dbc_env == "dev" or dbc_env is None:
    # dev src file path
    src_file_path = "conference-data-ap-south-1-landing-dev"
else:
    # Production src file
    src_file_path = "conference-data-ap-south-1-landing-prod"

EVENT = "event.csv"
SESSION = "session.csv"
POLLQUESTIONS = "polling_questions.csv"
INPERSONATTENDEE = "inperson_attendee.csv"
VIRTUALATTENDEE = "virtual_attendee.csv"
MODE = "append"
TIMEZONE = "est"

event_schema = StructType(
    [
        StructField("event_name", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("addr_line_1", StringType(), True),
        StructField("addr_line_2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zipcode", StringType(), True),
    ]
)

session_schema = StructType(
    [
        StructField("event_name", StringType(), True),
        StructField("session_title", StringType(), True),
        StructField("session_date", StringType(), True),
        StructField("speakers", StringType(), True),
        StructField("supporter", StringType(), True),
    ]
)

attendee_schema = StructType(
    [
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("credentials", StringType(), True),
        StructField("npi_number", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("email_address", StringType(), True),
        StructField("session_title", StringType(), True),
    ]
)

poll_question_schema = StructType(
    [
        StructField("poll_type", StringType(), True),
        StructField("parent_question", StringType(), True),
        StructField("poll_question", StringType(), True),
        StructField("poll_option", StringType(), True),
        StructField("count", StringType(), True),
        StructField("total_votes", StringType(), True),
        StructField("results", StringType(), True),
        StructField("survey_name", StringType(), True),
        StructField("session_title", StringType(), True),
    ]
)