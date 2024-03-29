# Databricks notebook source
from src.main import convert_date_object, read_data_from_raw
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
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

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils = DBUtils(spark)

# COMMAND ----------

require_processing = dbutils.jobs.taskValues.get(
    taskKey="schema_validation", key="execute_raw_layer", debugValue=True # Change it to False
)
if not require_processing:
    dbutils.notebook.exit("File not found! exiting the notebook!")
else:
    print("Proceeding to execute current notebook!")

# COMMAND ----------


require_processing = dbutils.jobs.taskValues.get(
    taskKey="source_to_raw", key="execute_refined_layer", debugValue=True # Change it to False
)

if not require_processing:
    dbutils.notebook.exit("Failed to load one or more files in raw layer! exiting the notebook!")
else:
    print("Proceeding to execute current notebook!")  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Tables

# COMMAND ----------

with open("../SqlDBM/src/Tables/conference_trusted.event_dim.sql") as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conference_trusted.session_dim.sql") as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conference_trusted.event_registrant_dim.sql") as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conference_trusted.attendee_session_dim.sql") as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conference_trusted.question_dim.sql") as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conference_trusted.question_attendee_dim.sql") as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conference_trusted.session_poll_fact.sql") as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

 with open("../SqlDBM/src/Tables/conference_trusted.satisfaction_rating.sql") as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conference_trusted.attendee_session_fact.sql") as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

with open("../SqlDBM/src/Tables/conference_trusted.event_attendee_fact.sql") as file:
    ddl = file.read()
    spark.sql(ddl)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pull Raw layer data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Event Table

# COMMAND ----------

event_df = spark.sql(
    """
  select
    event_id,
    lower(event_name) as event_name,
    lower(addr_line_1) as addr_line_1,
    lower(addr_line_2) as addr_line_2,
    lower(city) as city,
    lower(state) as state,
    zipcode,
    start_date,
    end_date,
    country,
    create_user
  from
    conference_refined.event
  where
    is_processed is false """
)

# COMMAND ----------

event_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Session Table

# COMMAND ----------

session_df = spark.sql(
    """
select
  session_id,
  lower(event_name) as event_name,
  lower(session_title) as session_title,
  lower(speakers) as speakers,
  lower(supporter) as supporter,
  start_time,
  end_time,
  exact_start_time,
  exact_end_time,
  session_date
from
  conference_refined.session
where
  is_processed is false"""
)

# COMMAND ----------

session_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Registrants

# COMMAND ----------

registrants_df = spark.sql(
    """
  select
    attendee_id,
    registration_no,
    lower(first_name) as first_name,
    lower(last_name) as last_name,
    lower(job_role) as job_role,
    lower(state) as state,
    email_address,
    login_time,
    logout_time,
    lower(session_title) as session_title,
    attendee_type,
    create_user
  from conference_refined.registrant where is_processed is false"""
)

# COMMAND ----------

registrants_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Poll Question Table

# COMMAND ----------

poll_questions_df = spark.sql(
    """
    SELECT 
        poll_question,
        poll_option,
        option_text,
        attendee_registration_no,
        session_title,
        create_date,
        create_user,
        session_title
    FROM conference_refined.polling_questions
    WHERE is_processed is False
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

display(event_df.head(5))

# COMMAND ----------

event_df.createOrReplaceTempView("new_event_temp_view")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Session

# COMMAND ----------

display(session_df.head(5))

# COMMAND ----------

session_df.createOrReplaceTempView("new_session_temp_view")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Attendee

# COMMAND ----------

display(registrants_df.count())

# COMMAND ----------

registrants_df.createOrReplaceTempView("new_attendee_master_temp_view")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load

# COMMAND ----------

# MAGIC %md
# MAGIC #### Event

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO conference_trusted.event_dim AS des USING (
# MAGIC   SELECT
# MAGIC     event_id,
# MAGIC     event_name,
# MAGIC     addr_line_1,
# MAGIC     addr_line_2,
# MAGIC     city,
# MAGIC     state,
# MAGIC     zipcode,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     country,
# MAGIC     create_user
# MAGIC   FROM
# MAGIC     new_event_temp_view
# MAGIC ) AS src ON src.event_name = des.event_name
# MAGIC AND src.start_date = des.start_date
# MAGIC AND src.end_date = des.end_date
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   des.addr_line_1 = src.addr_line_1,
# MAGIC   des.addr_line_2 = src.addr_line_2,
# MAGIC   des.city = src.city,
# MAGIC   des.state = src.state,
# MAGIC   des.zipcode = src.zipcode,
# MAGIC   des.create_user = src.create_user,
# MAGIC   des.country = src.country,
# MAGIC   des.modified_date = current_timestamp()
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   (
# MAGIC     event_id,
# MAGIC     event_name,
# MAGIC     addr_line_1,
# MAGIC     addr_line_2,
# MAGIC     city,
# MAGIC     state,
# MAGIC     zipcode,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     create_user,
# MAGIC     create_date,
# MAGIC     country
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     src.event_id,
# MAGIC     src.event_name,
# MAGIC     src.addr_line_1,
# MAGIC     src.addr_line_1,
# MAGIC     src.city,
# MAGIC     src.state,
# MAGIC     src.zipcode,
# MAGIC     src.start_date,
# MAGIC     src.end_date,
# MAGIC     src.create_user,
# MAGIC     current_date(),
# MAGIC     src.country
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace temp view event_dim_temp_view as
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   conference_trusted.event_dim
# MAGIC where
# MAGIC   create_date = current_date
# MAGIC   or modified_date = current_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace temp view session_temp_view as
# MAGIC select
# MAGIC   e.event_id,
# MAGIC   s.*
# MAGIC from
# MAGIC   new_session_temp_view s
# MAGIC   left join event_dim_temp_view e on s.event_name = e.event_name

# COMMAND ----------

# MAGIC %md
# MAGIC #### Session

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO conference_trusted.session_dim AS des USING (
# MAGIC   SELECT
# MAGIC     event_id,
# MAGIC     event_name,
# MAGIC     session_id,
# MAGIC     session_title,
# MAGIC     speakers,
# MAGIC     session_date,
# MAGIC     supporter,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC     exact_start_time,
# MAGIC     exact_end_time
# MAGIC   FROM
# MAGIC     session_temp_view
# MAGIC ) AS src ON des.session_title = src.session_title
# MAGIC AND des.session_date = src.session_date
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   des.speakers = src.speakers,
# MAGIC   des.supporter = src.supporter,
# MAGIC   des.start_time = src.start_time,
# MAGIC   des.end_time = src.end_time,
# MAGIC   des.exact_start_time = src.exact_end_time,
# MAGIC   des.create_user = current_user(),
# MAGIC   des.modified_date = current_timestamp()
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   (
# MAGIC     event_id,
# MAGIC     event_name,
# MAGIC     session_id,
# MAGIC     session_title,
# MAGIC     session_date,
# MAGIC     speakers,
# MAGIC     supporter,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC     exact_start_time,
# MAGIC     exact_end_time,
# MAGIC     create_user,
# MAGIC     create_date
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     src.event_id,
# MAGIC     src.event_name,
# MAGIC     src.session_id,
# MAGIC     src.session_title,
# MAGIC     src.session_date,
# MAGIC     src.speakers,
# MAGIC     src.supporter,
# MAGIC     src.start_time,
# MAGIC     src.end_time,
# MAGIC     src.exact_start_time,
# MAGIC     src.exact_end_time,
# MAGIC     current_user(),
# MAGIC     current_date()
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace temp view session_dim_temp_view as
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   conference_trusted.session_dim
# MAGIC where
# MAGIC   create_date = current_date
# MAGIC   or modified_date = current_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from session_dim_temp_view;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Attendee

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace temp view attendee_temp_view as
# MAGIC select
# MAGIC   distinct
# MAGIC   registration_no,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email_address,
# MAGIC   job_role,
# MAGIC   t.state,
# MAGIC   e.event_id as event_id,
# MAGIC   e.event_name
# MAGIC from
# MAGIC   new_attendee_master_temp_view t
# MAGIC   left join event_dim_temp_view e

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   attendee_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO conference_trusted.event_registrant_dim as des USING (
# MAGIC   SELECT
# MAGIC     registration_no,
# MAGIC     event_id,
# MAGIC     event_name,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     state,
# MAGIC     job_role,
# MAGIC     email_address
# MAGIC   FROM
# MAGIC     attendee_temp_view
# MAGIC ) AS src ON src.email_address = des.email_address
# MAGIC AND src.first_name = des.first_name
# MAGIC AND src.last_name = des.last_name
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   des.job_role = src.job_role,
# MAGIC   des.state = src.state,
# MAGIC   des.modified_date = current_timestamp()
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT(
# MAGIC     event_id,
# MAGIC     event_name,
# MAGIC     registration_no,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     job_role,
# MAGIC     state,
# MAGIC     email_address,
# MAGIC     create_user,
# MAGIC     create_date
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     src.event_id,
# MAGIC     src.event_name,
# MAGIC     src.registration_no,
# MAGIC     src.first_name,
# MAGIC     src.last_name,
# MAGIC     src.job_role,
# MAGIC     src.state,
# MAGIC     src.email_address,
# MAGIC     current_user(),
# MAGIC     current_date()
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   conference_trusted.event_registrant_dim
# MAGIC where
# MAGIC   create_date = current_date
# MAGIC   or modified_date = current_date

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace temp view attendee_dim_temp_view as
# MAGIC SELECT
# MAGIC   d.*,
# MAGIC   t.attendee_type,
# MAGIC   t.session_title,
# MAGIC   t.login_time,
# MAGIC   t.logout_time
# MAGIC FROM
# MAGIC   conference_trusted.event_registrant_dim d
# MAGIC   LEFT JOIN new_attendee_master_temp_view t on t.registration_no = d.registration_no
# MAGIC where
# MAGIC   t.session_title is not null
# MAGIC   and d.create_date = current_date
# MAGIC   or d.modified_date = current_date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC from
# MAGIC   attendee_dim_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into
# MAGIC   conference_trusted.attendee_session_dim
# MAGIC select
# MAGIC   s.session_id,
# MAGIC   s.session_title,
# MAGIC   a.attendee_id,
# MAGIC   a.attendee_type,
# MAGIC   login_time,
# MAGIC   logout_time,
# MAGIC   current_date() as create_date,
# MAGIC   current_user()
# MAGIC from
# MAGIC   attendee_dim_temp_view a
# MAGIC   left join session_dim_temp_view s on s.session_title = a.session_title

# COMMAND ----------

# MAGIC %md
# MAGIC #### Questions

# COMMAND ----------

poll_questions_df.createOrReplaceTempView("polling_questions_master_temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMP VIEW questions as
# MAGIC Select
# MAGIC   DISTINCT poll_question
# MAGIC from
# MAGIC   polling_questions_master_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC from
# MAGIC   questions;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO conference_trusted.question_dim as des USING (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   from
# MAGIC     questions
# MAGIC ) AS src ON src.poll_question = des.question_text
# MAGIC WHEN MATCHED then
# MAGIC update
# MAGIC set
# MAGIC   des.modified_date = current_date(),
# MAGIC   des.modified_user = current_user()
# MAGIC   when not matched then
# MAGIC insert
# MAGIC   (question_text, create_user, create_date)
# MAGIC values
# MAGIC   (
# MAGIC     src.poll_question,
# MAGIC     current_user(),
# MAGIC     current_date()
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   q.question_id,
# MAGIC   er.attendee_id,
# MAGIC   s.session_id,
# MAGIC   s.session_title,
# MAGIC   t.option_text
# MAGIC FROM
# MAGIC   polling_questions_master_temp_view t
# MAGIC   LEFT JOIN conference_trusted.question_dim q on q.question_text = t.poll_question
# MAGIC   LEFT JOIN conference_trusted.event_registrant_dim er on er.registration_no = t.attendee_registration_no
# MAGIC   LEFT JOIN session_dim_temp_view s on s.session_title = t.session_title
# MAGIC WHERE
# MAGIC   q.create_date = current_date
# MAGIC   or q.modified_date = current_date

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   conference_trusted.question_attendee_dim
# MAGIC SELECT
# MAGIC   s.session_id,
# MAGIC   s.session_title,
# MAGIC   er.attendee_id,
# MAGIC   q.question_id,
# MAGIC   q.question_text,
# MAGIC   t.option_text,
# MAGIC   current_date() as create_date,
# MAGIC   current_user() as create_user
# MAGIC FROM
# MAGIC   polling_questions_master_temp_view t
# MAGIC   LEFT JOIN conference_trusted.question_dim q on q.question_text = t.poll_question
# MAGIC   LEFT JOIN conference_trusted.event_registrant_dim er on er.registration_no = t.attendee_registration_no
# MAGIC   LEFT JOIN session_dim_temp_view s on s.session_title = t.session_title
# MAGIC WHERE
# MAGIC   q.create_date = current_date
# MAGIC   or q.modified_date = current_date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   first(t.poll_question) as poll_question,
# MAGIC   q.question_id,
# MAGIC   count(q.question_id) as question_response_count,
# MAGIC   first(s.session_id) as session_id,
# MAGIC   first(s.session_title) as session_title
# MAGIC from
# MAGIC   polling_questions_master_temp_view t
# MAGIC   LEFT JOIN conference_trusted.question_dim q on q.question_text = t.poll_question
# MAGIC   LEFT JOIN session_dim_temp_view s on s.session_title = t.session_title
# MAGIC GROUP BY
# MAGIC   q.question_id,
# MAGIC   s.session_id

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO conference_trusted.session_poll_fact 
# MAGIC SELECT
# MAGIC   q.question_id,
# MAGIC   first(t.poll_question) as question_text,
# MAGIC   first(s.session_id) as session_id,
# MAGIC   first(s.session_title) as session_title,
# MAGIC   count(q.question_id) as question_response_cout,
# MAGIC   current_date() as create_date,
# MAGIC   current_user() as create_user
# MAGIC from
# MAGIC   polling_questions_master_temp_view t
# MAGIC   LEFT JOIN conference_trusted.question_dim q on q.question_text = t.poll_question
# MAGIC   LEFT JOIN session_dim_temp_view s on s.session_title = t.session_title
# MAGIC GROUP BY
# MAGIC   q.question_id,
# MAGIC   s.session_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from conference_trusted.question_dim;

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace temp view satisfaction_temp_view as
# MAGIC SELECT
# MAGIC   s.session_id,
# MAGIC   q.question_id,
# MAGIC   poll_question,
# MAGIC   poll_option,
# MAGIC   option_text,
# MAGIC   attendee_registration_no,
# MAGIC   s.session_title
# MAGIC from
# MAGIC   polling_questions_master_temp_view t
# MAGIC   left join conference_trusted.question_dim q on q.question_text = t.poll_question
# MAGIC   and t.poll_question = 'How would you like to rate the session out of five?'
# MAGIC   left join session_dim_temp_view s on s.session_title = t.session_title
# MAGIC where
# MAGIC   q.create_date = current_date
# MAGIC   or q.modified_date = current_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from satisfaction_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT into conference_trusted.satisfaction_rating
# MAGIC SELECT
# MAGIC   first(session_id) as session_id,
# MAGIC   first(session_title) as session_title,
# MAGIC   first(question_id) as question_id,
# MAGIC   first(poll_question) as question_text,
# MAGIC   first(cast(poll_option as INT)) as rating,
# MAGIC   count(option_text) as rating_count,
# MAGIC   5 as target_rating,
# MAGIC   current_date() as create_date,
# MAGIC   current_user() as create_user
# MAGIC from
# MAGIC   satisfaction_temp_view
# MAGIC GROUP BY
# MAGIC   session_id,
# MAGIC   option_text

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from conference_trusted.satisfaction_rating

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC or Replace temp view dist_attendee_event_view as
# MAGIC SELECT
# MAGIC   count(*) as attendee_count,
# MAGIC   event_id,
# MAGIC   first(event_name) as event_name
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       DISTINCT s.event_id,
# MAGIC       s.event_name,
# MAGIC       a.attendee_id
# MAGIC     FROM
# MAGIC       conference_trusted.attendee_session_dim a
# MAGIC       LEFT JOIN session_dim_temp_view s on s.session_id = a.session_id
# MAGIC     where
# MAGIC       a.create_date = current_date
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   event_id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC or replace temp view dist_registrant_event_view as
# MAGIC SELECT
# MAGIC   count(*) as registrant_count,
# MAGIC   event_id
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       DISTINCT event_id,
# MAGIC       attendee_id
# MAGIC     FROM
# MAGIC       conference_trusted.event_registrant_dim
# MAGIC     where
# MAGIC       create_date = current_date
# MAGIC       or modified_date = current_date
# MAGIC   )
# MAGIC group by
# MAGIC   event_id

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from dist_attendee_event_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dist_registrant_event_view

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   conference_trusted.event_attendee_fact
# MAGIC SELECT
# MAGIC   a.event_id,
# MAGIC   a.event_name,
# MAGIC   r.registrant_count,
# MAGIC   a.attendee_count,
# MAGIC   current_date() as create_date,
# MAGIC   current_user() as create_user
# MAGIC from
# MAGIC   dist_attendee_event_view a
# MAGIC   INNER JOIN dist_registrant_event_view r on r.event_id = a.event_id

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   conference_trusted.attendee_session_fact
# MAGIC SELECT
# MAGIC   session_id,
# MAGIC   session_title,
# MAGIC   (
# MAGIC     SELECT
# MAGIC       count(*)
# MAGIC     from
# MAGIC       conference_trusted.attendee_session_dim
# MAGIC     where
# MAGIC       create_date = current_date
# MAGIC   ) as event_total_attendee,
# MAGIC   count(attendee_id) as session_total_attendee,
# MAGIC   current_date() as create_date,
# MAGIC   current_user() as current_user
# MAGIC from
# MAGIC   conference_trusted.attendee_session_dim
# MAGIC where
# MAGIC   create_date = current_date
# MAGIC GROUP BY
# MAGIC   session_id,
# MAGIC   session_title

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE conference_refined.event SET modified_date = current_date(), modified_user = current_user(), is_processed = TRUE WHERE is_processed = FALSE;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE conference_refined.session SET modified_date = current_date(), modified_user = current_user(), is_processed = TRUE WHERE is_processed = FALSE;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE conference_refined.registrant SET modified_date = current_date(), modified_user = current_user(), is_processed = TRUE WHERE is_processed = FALSE;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE conference_refined.polling_questions SET modified_date = current_date(), modified_user = current_user(), is_processed = TRUE WHERE is_processed = FALSE;

# COMMAND ----------


