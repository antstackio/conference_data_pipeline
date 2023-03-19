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

event_df = spark.sql(
    """
  select
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
    conference_raw.event
  where
    is_processed is false """
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Session Table

# COMMAND ----------

session_df = spark.sql(
    """
select
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
  conference_raw.session
where
  is_processed is false"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inperson Attendee Table

# COMMAND ----------

inperson_attendee_df = spark.sql(
    """
  select
    registration_no,
    lower(first_name) as first_name,
    lower(last_name) as last_name,
    lower(job_role) as job_role,
    lower(s.State) as state,
    email_address,
    create_user,
    lower(session_title) as session_title,
    'inperson' as attendee_type 
  from conference_raw.in_person_attendee i left join default.states s on s.Abbreviation = i.state where i.is_processed is false"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Virtual Attendee Table

# COMMAND ----------

virtual_attendee_df = spark.sql(
    """
  select 
    registration_no,
    lower(first_name) as first_name,
    lower(last_name) as last_name,
    lower(job_role) as job_role,
    lower(s.State) as state,
    email_address,
    create_user,
    lower(session_title) as session_title,
    'virtual' as attendee_type 
  from conference_raw.virtual_attendee v left join default.states s on s.Abbreviation = v.state where v.is_processed is false"""
)

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
    FROM conference_raw.polling_questions
    WHERE is_processed is False
    """
)

# COMMAND ----------

event_df = convert_date_object(event_df, "start_date", "dd/MM/y")
display(event_df)

# COMMAND ----------

event_df = convert_date_object(event_df, "end_date", "dd/MM/y")
display(event_df)

# COMMAND ----------

event_df.createOrReplaceTempView("new_event_temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO conference_refined.event_dim AS des USING (
# MAGIC   SELECT
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
# MAGIC   des.zip_code = src.zipcode,
# MAGIC   des.create_user = src.create_user,
# MAGIC   des.country = src.country,
# MAGIC   des.modified_date = current_timestamp()
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   (
# MAGIC     event_name,
# MAGIC     addr_line_1,
# MAGIC     addr_line_2,
# MAGIC     city,
# MAGIC     state,
# MAGIC     zip_code,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     create_user,
# MAGIC     create_date,
# MAGIC     country
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
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
# MAGIC   conference_refined.event_dim
# MAGIC where
# MAGIC   create_date = current_date
# MAGIC   or modified_date = current_date;

# COMMAND ----------

session_df = convert_date_object(session_df, "session_date", "d/M/y")
display(session_df)

# COMMAND ----------

session_df.createOrReplaceTempView("new_session_temp_view")

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

# MAGIC %sql
# MAGIC MERGE INTO conference_refined.session_dim AS des USING (
# MAGIC   SELECT
# MAGIC     event_id,
# MAGIC     event_name,
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
# MAGIC   conference_refined.session_dim
# MAGIC where
# MAGIC   create_date = current_date
# MAGIC   or modified_date = current_date;

# COMMAND ----------

display(inperson_attendee_df.count())

# COMMAND ----------

display(virtual_attendee_df.count())

# COMMAND ----------

attendee = inperson_attendee_df.union(virtual_attendee_df)

# COMMAND ----------

display(attendee.count())

# COMMAND ----------

attendee.createOrReplaceTempView("new_attendee_master_temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace temp view attendee_temp_view as
# MAGIC select
# MAGIC   distinct first_name,
# MAGIC   last_name,
# MAGIC   email_address,
# MAGIC   job_role,
# MAGIC   state
# MAGIC from
# MAGIC   new_attendee_master_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   attendee_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO conference_refined.attendee_dim as des USING (
# MAGIC   SELECT
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
# MAGIC   conference_refined.attendee_dim
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
# MAGIC   t.session_title
# MAGIC FROM
# MAGIC   conference_refined.attendee_dim d
# MAGIC   LEFT JOIN new_attendee_master_temp_view t ON t.first_name = d.first_name
# MAGIC   AND t.last_name = d.last_name
# MAGIC   AND t.job_role = d.job_role
# MAGIC   AND t.state = d.state
# MAGIC where
# MAGIC   d.create_date = current_date
# MAGIC   or d.modified_date = current_date
# MAGIC order by
# MAGIC   d.attendee_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC from
# MAGIC   attendee_dim_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into
# MAGIC   conference_refined.attendee_session_dim
# MAGIC select
# MAGIC   s.session_id,
# MAGIC   a.attendee_id,
# MAGIC   a.attendee_type,
# MAGIC   current_date() as create_date
# MAGIC from
# MAGIC   attendee_dim_temp_view a
# MAGIC   left join session_dim_temp_view s on s.session_title = a.session_title

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
# MAGIC MERGE INTO conference_refined.questions_dim as des USING (
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
# MAGIC INSERT INTO
# MAGIC   conference_refined.question_attendee_dim
# MAGIC SELECT
# MAGIC   q.question_id,
# MAGIC   a.attendee_id,
# MAGIC   t.option_text,
# MAGIC   current_date() as create_date,
# MAGIC   current_user() as create_user
# MAGIC FROM
# MAGIC   polling_questions_master_temp_view t
# MAGIC   LEFT JOIN conference_refined.questions_dim q on q.question_text = t.poll_question
# MAGIC   AND q.create_date = current_date
# MAGIC   or q.modified_date = current_date
# MAGIC   LEFT JOIN new_attendee_master_temp_view at on at.registration_no = t.attendee_registration_no
# MAGIC   LEFT JOIN conference_refined.attendee_dim a on a.first_name = at.first_name
# MAGIC   and a.last_name = at.last_name
# MAGIC   and a.job_role = at.job_role
# MAGIC   AND a.state = at.state

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   conference_refined.questions_dim;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC or REPLACE TEMP VIEW question_agg as
# MAGIC SELECT
# MAGIC   first(t.poll_question) as poll_question,
# MAGIC   q.question_id,
# MAGIC   count(q.question_id) as question_response_count
# MAGIC from
# MAGIC   polling_questions_master_temp_view t
# MAGIC   LEFT JOIN conference_refined.questions_dim q on q.question_text = t.poll_question
# MAGIC GROUP BY
# MAGIC   q.question_id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC or REPLACE TEMP VIEW option_agg as
# MAGIC SELECT
# MAGIC   first(poll_option) as poll_option,
# MAGIC   first(option_text) as option_text,
# MAGIC   count(option_text) as option_count
# MAGIC FROM
# MAGIC   polling_questions_master_temp_view
# MAGIC GROUP BY
# MAGIC   option_text,
# MAGIC   poll_option

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC or replace temp view question_options_temp_view as
# MAGIC SELECT
# MAGIC   DISTINCT poll_question,
# MAGIC   poll_option,
# MAGIC   option_text,
# MAGIC   session_title
# MAGIC from
# MAGIC   polling_questions_master_temp_view
# MAGIC WHERE
# MAGIC   option_text != 'null'
# MAGIC ORDER BY
# MAGIC   poll_question,
# MAGIC   poll_option

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC from
# MAGIC   question_options_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC from
# MAGIC   polling_questions_master_temp_view
# MAGIC WHERE
# MAGIC   session_title = 'null';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   conference_refined.event_poll_fact
# MAGIC SELECT
# MAGIC   pa.question_id,
# MAGIC   pt.option_text,
# MAGIC   pa.question_response_count,
# MAGIC   qa.option_count,
# MAGIC   s.session_id,
# MAGIC   current_date() as create_date,
# MAGIC   current_user() as create_user
# MAGIC FROM
# MAGIC   question_options_temp_view pt
# MAGIC   LEFT JOIN question_agg pa on pt.poll_question = pa.poll_question
# MAGIC   LEFT JOIN option_agg qa on qa.option_text = pt.option_text
# MAGIC   AND qa.poll_option = pt.poll_option
# MAGIC   LEFT JOIN session_dim_temp_view s on s.session_title = pt.session_title
# MAGIC ORDER BY
# MAGIC   pt.poll_question

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC from
# MAGIC   conference_refined.event_poll_fact

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from conference_refined.attendee_dim;

# COMMAND ----------


