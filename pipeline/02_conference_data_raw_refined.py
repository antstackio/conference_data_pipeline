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
)


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
    create_user
  from
    conference_raw.event
  where
    is_processed is false """
  )

# COMMAND ----------

event_df = convert_date_object(event_df, 'start_date', 'dd/MM/y')
display(event_df)

# COMMAND ----------

event_df = convert_date_object(event_df, 'end_date', 'dd/MM/y')
display(event_df)

# COMMAND ----------

event_df.createOrReplaceTempView("new_event_temp_view")

# COMMAND ----------

# MAGIC %sql MERGE INTO conference_refined.event_dim AS des USING (
# MAGIC   SELECT
# MAGIC     event_name,
# MAGIC     event_type,
# MAGIC     addr_line_1,
# MAGIC     addr_line_2,
# MAGIC     city,
# MAGIC     state,
# MAGIC     zipcode,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     create_user
# MAGIC   FROM
# MAGIC     new_event_temp_view
# MAGIC ) AS src ON src.event_name = des.event_name
# MAGIC AND src.start_date = des.start_date
# MAGIC AND src.end_date = des.end_date
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE
# MAGIC   SET
# MAGIC     des.event_type = src.event_type,
# MAGIC     des.addr_line_1 = src.addr_line_1,
# MAGIC     des.addr_line_2 = src.addr_line_2,
# MAGIC     des.city = src.city,
# MAGIC     des.state = src.state,
# MAGIC     des.zip_code = src.zipcode,
# MAGIC     des.create_user = src.create_user,
# MAGIC     des.modified_date = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT(
# MAGIC       event_name,
# MAGIC       event_type,
# MAGIC       addr_line_1,
# MAGIC       addr_line_2,
# MAGIC       city,
# MAGIC       state,
# MAGIC       zip_code,
# MAGIC       start_date,
# MAGIC       end_date,
# MAGIC       create_user,
# MAGIC       create_date
# MAGIC     )
# MAGIC   VALUES
# MAGIC     (
# MAGIC       src.event_name,
# MAGIC       src.event_type,
# MAGIC       src.addr_line_1,
# MAGIC       src.addr_line_1,
# MAGIC       src.city,
# MAGIC       src.state,
# MAGIC       src.zipcode,
# MAGIC       src.start_date,
# MAGIC       src.end_date,
# MAGIC       src.create_user,
# MAGIC       current_date()
# MAGIC     )

# COMMAND ----------

# MAGIC %sql create
# MAGIC or replace temp view event_dim_temp_view as
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   medical_refined.event_dim
# MAGIC where
# MAGIC   create_date = current_date
# MAGIC   or modified_date = current_date;

# COMMAND ----------

session_df = spark.sql(
    """
select
  lower(event_name) as event_name,
  lower(session_title) as session_title,
  lower(speakers) as speakers,
  lower(supporter) as supporter,
  session_date
from
  medical_raw.session
where
  is_processed is false"""
)

# COMMAND ----------

session_df = session_df.withColumn(
    "session_date",
    when(
        to_date(session_df.session_date, "dd/mm/yyyy").isNotNull(),
        to_date(session_df.session_date, "dd/mm/yyyy"),
    ).otherwise(to_date(session_df.session_date)),
)

display(session_df)

# COMMAND ----------

session_df.createOrReplaceTempView("new_session_temp_view")

# COMMAND ----------

# MAGIC %sql create
# MAGIC or replace temp view session_temp_view as
# MAGIC select
# MAGIC   e.event_id,
# MAGIC   s.*
# MAGIC from
# MAGIC   new_session_temp_view s
# MAGIC   left join event_dim_temp_view e on s.event_name = e.event_name

# COMMAND ----------

# MAGIC %sql MERGE INTO medical_refined.session_dim AS des USING (
# MAGIC   SELECT
# MAGIC     event_id,
# MAGIC     event_name,
# MAGIC     session_title,
# MAGIC     speakers,
# MAGIC     session_date,
# MAGIC     supporter
# MAGIC   FROM
# MAGIC     session_temp_view
# MAGIC ) AS src ON des.session_title = src.session_title
# MAGIC AND des.session_date = src.session_date
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   des.event_id = src.event_id,
# MAGIC   des.event_name = src.event_name,
# MAGIC   des.speakers = src.speakers,
# MAGIC   des.supporter = src.supporter,
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
# MAGIC     current_user(),
# MAGIC     current_date()
# MAGIC   )

# COMMAND ----------

# MAGIC %sql create
# MAGIC or replace temp view session_dim_temp_view as
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   medical_refined.session_dim
# MAGIC where
# MAGIC   create_date = current_date
# MAGIC   or modified_date = current_date;

# COMMAND ----------

inperson_attendee_df = spark.sql(
    """
  select
    npi_number,
    lower(first_name) as first_name,
    lower(last_name) as last_name,
    lower(credentials) as credentials,
    lower(city) as city,
    lower(state) as state,
    zip_code,
    email_address,
    create_user,
    lower(session_title) as session_title,
    'inperson' as attendee_type 
  from medical_raw.in_person_attendee where is_processed is false"""
)

# COMMAND ----------

virtual_attendee_df = spark.sql(
    """
  select 
    npi_number,
    lower(first_name) as first_name,
    lower(last_name) as last_name,
    lower(credentials) as credentials,
    lower(city) as city,
    lower(state) as state,
    zip_code,
    email_address,
    create_user,
    lower(session_title) as session_title,
    'virtual' as attendee_type 
  from medical_raw.virtual_attendee where is_processed is false"""
)

# COMMAND ----------

display(inperson_attendee_df.head(10))


# COMMAND ----------

display(virtual_attendee_df.head(10))


# COMMAND ----------

attendee = inperson_attendee_df.union(virtual_attendee_df)


# COMMAND ----------

attendee.createOrReplaceTempView("new_attendee_master_temp_view")


# COMMAND ----------

# MAGIC %sql create
# MAGIC or replace temp view attendee_temp_view as
# MAGIC select
# MAGIC   distinct npi_number,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email_address,
# MAGIC   credentials,
# MAGIC   city,
# MAGIC   state,
# MAGIC   zip_code,
# MAGIC   create_user
# MAGIC from
# MAGIC   new_attendee_master_temp_view;

# COMMAND ----------

# MAGIC %sql MERGE INTO medical_refined.attendee_dim as des USING (
# MAGIC   SELECT
# MAGIC     npi_number,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     veeva_network_master_id,
# MAGIC     customer_id,
# MAGIC     specialty_1,
# MAGIC     specialty_2,
# MAGIC     addr_line_1,
# MAGIC     addr_line_2,
# MAGIC     addr_line_3,
# MAGIC     country,
# MAGIC     postal_cd,
# MAGIC     city,
# MAGIC     state,
# MAGIC     zip_code,
# MAGIC     credentials,
# MAGIC     email_address,
# MAGIC     create_user
# MAGIC   FROM
# MAGIC     attendee_temp_view
# MAGIC ) AS src ON src.npi_number = des.npi_number
# MAGIC AND src.email_address = des.email_address
# MAGIC AND src.first_name = des.first_name
# MAGIC AND src.last_name = des.last_name
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   des.credentials = src.credentials,
# MAGIC   des.city = src.city,
# MAGIC   des.state = src.state,
# MAGIC   des.zip_code = src.zip_code,
# MAGIC   des.create_user = src.create_user,
# MAGIC   des.modified_date = current_timestamp()
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT(
# MAGIC     npi_number,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     veeva_network_master_id,
# MAGIC     customer_id,
# MAGIC     specialty_1,
# MAGIC     specialty_2,
# MAGIC     addr_line_1,
# MAGIC     addr_line_2,
# MAGIC     addr_line_3,
# MAGIC     country,
# MAGIC     postal_cd,
# MAGIC     credentials,
# MAGIC     city,
# MAGIC     state,
# MAGIC     zip_code,
# MAGIC     email_address,
# MAGIC     create_user,
# MAGIC     create_date
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     src.npi_number,
# MAGIC     src.first_name,
# MAGIC     src.last_name,
# MAGIC     src.veeva_network_master_id,
# MAGIC     src.customer_id,
# MAGIC     src.specialty_1,
# MAGIC     src.specialty_2,
# MAGIC     src.addr_line_1,
# MAGIC     src.addr_line_2,
# MAGIC     src.addr_line_3,
# MAGIC     src.country,
# MAGIC     src.postal_cd,
# MAGIC     src.credentials,
# MAGIC     src.city,
# MAGIC     src.state,
# MAGIC     src.zip_code,
# MAGIC     src.email_address,
# MAGIC     src.create_user,
# MAGIC     current_date()
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   medical_refined.attendee_dim
# MAGIC where
# MAGIC   create_date = current_date
# MAGIC   or modified_date = current_date%sql create
# MAGIC or replace temp view attendee_dim_temp_view as
# MAGIC SELECT
# MAGIC   d.*,
# MAGIC   t.attendee_type,
# MAGIC   t.session_title
# MAGIC FROM
# MAGIC   medical_refined.attendee_dim d
# MAGIC   LEFT JOIN new_attendee_master_temp_view t ON t.npi_number = d.npi_number
# MAGIC   AND t.first_name = d.first_name
# MAGIC   AND t.last_name = d.last_name
# MAGIC   AND t.email_address = d.email_address
# MAGIC where
# MAGIC   d.create_date = current_date
# MAGIC   or d.modified_date = current_date
# MAGIC order by
# MAGIC   d.attendee_id

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into
# MAGIC   medical_refined.attendee_session_dim
# MAGIC select
# MAGIC   s.session_id,
# MAGIC   a.attendee_id,
# MAGIC   a.attendee_type,
# MAGIC   current_date() as create_date
# MAGIC from
# MAGIC   attendee_dim_temp_view a
# MAGIC   left join session_dim_temp_view s on s.session_title = a.session_title
