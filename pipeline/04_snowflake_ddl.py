# Databricks notebook source
from src.snowflake_credentials import *

sfUtils = spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils

# COMMAND ----------

options = snowflake_options('trusted')

# COMMAND ----------

drop_query = [
"drop table if exists CONFORMED.ATTENDEE_SESSION_DIM;",
"drop table if exists CONFORMED.EVENT_ATTENDEE_FACT;",
"drop table if exists CONFORMED.EVENT_DIM;",
"drop table if exists CONFORMED.EVENT_REGISTRANT_DIM;",
"drop table if exists CONFORMED.QUESTION_DIM;",
"drop table if exists CONFORMED.QUESTION_ATTENDEE_DIM;",
"drop table if exists CONFORMED.SATISFACTION_RATING;",
"drop table if exists CONFORMED.SESSION_DIM;",
"drop table if exists CONFORMED.ATTENDEE_SESSION_FACT",
"drop table if exists CONFORMED.SESSION_POLL_FACT"
]

for query in drop_query:
    sfUtils.runQuery(options, query)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conformed.event_dim.sql") as file:
    ddl = file.read()
    sfUtils.runQuery(options, ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conformed.event_attendee_fact.sql") as file:
    ddl = file.read()
    sfUtils.runQuery(options, ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conformed.question_dim.sql") as file:
    ddl = file.read()
    sfUtils.runQuery(options, ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conformed.session_dim.sql") as file:
    ddl = file.read()
    sfUtils.runQuery(options, ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conformed.session_poll_fact.sql") as file:
    ddl = file.read()
    sfUtils.runQuery(options, ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conformed.attendee_session_dim.sql") as file:
    ddl = file.read()
#     print(ddl)
    sfUtils.runQuery(options, ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conformed.attendee_session_fact.sql") as file:
    ddl = file.read()
    sfUtils.runQuery(options, ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conformed.event_registrant_dim.sql") as file:
    ddl = file.read()
    sfUtils.runQuery(options, ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conformed.question_attendee_dim.sql") as file:
    ddl = file.read()
    sfUtils.runQuery(options, ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conformed.satisfaction_rating.sql") as file:
    ddl = file.read()
    sfUtils.runQuery(options, ddl)

# COMMAND ----------


