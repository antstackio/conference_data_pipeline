import os
from pyspark.dbutils import DBUtils
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    DateType,
    TimestampType,
    FloatType
)

dbc_env = os.getenv("dbc_environment")

if dbc_env == "dev" or dbc_env is None:
    # dev src file path
    bucket = "conference-data-ap-south-1-landing-dev/"
    src_file_path = bucket + 'input/'
elif dbc_env == 'stage':
    bucket = "conference-data-ap-south-1-landing-stage/"
    src_file_path = bucket + 'input/'
else:
    # Production src file
    bucket = "conference-data-ap-south-1-landing-prod/"
    src_file_path = bucket + 'input/'

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
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("addr_line_1", StringType(), True),
        StructField("addr_line_2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zipcode", StringType(), True),
        StructField("country", StringType(), True)
    ]
)

session_schema = StructType(
    [
        StructField("session_title", StringType(), True),
        StructField("session_date", StringType(), True),
        StructField("speakers", StringType(), True),
        StructField("supporter", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("exact_start_time", StringType(), True),
        StructField("exact_end_time", StringType(), True)
    ]
)

inperson_attendee_schema = StructType(
    [   
        StructField("registration_no", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email_address", StringType(), True),
        StructField("job_role", StringType(), True),
        StructField("state", StringType(), True),
        StructField("session_title", StringType(), True),
    ]
)

virtual_attendee_schema = StructType(
    [   
        StructField("registration_no", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email_address", StringType(), True),
        StructField("job_role", StringType(), True),
        StructField("state", StringType(), True),
        StructField("session_title", StringType(), True),
        StructField("login_time", StringType(), True),
        StructField("logout_time", StringType(), True),
    ]
)
poll_question_schema = StructType(
    [
        StructField("poll_question", StringType(), True),
        StructField("poll_option", StringType(), True),
        StructField("attendee_registration_no", StringType(), True),
        StructField("option_text", StringType(), True),
        StructField("session_title", StringType(), True),
    ]
)