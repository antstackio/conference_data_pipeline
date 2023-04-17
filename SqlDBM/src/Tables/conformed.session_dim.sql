-- ************************** SqlDBM: Snowflake *************************
-- ************************* Generated by SqlDBM ************************


-- ************************************** conformed.session_dim
CREATE TABLE IF NOT EXISTS conformed.session_dim
(
 session_id       bigint NOT NULL,
 session_title    string NOT NULL,
 event_id         bigint NOT NULL,
 event_name       string NOT NULL,
 speakers         string NOT NULL,
 supporter        string NOT NULL,
 session_date     date NOT NULL,
 start_time       TIMESTAMP NOT NULL,
 end_time         TIMESTAMP NOT NULL,
 exact_start_time TIMESTAMP NOT NULL,
 exact_end_time   TIMESTAMP NOT NULL,
 create_user      string NOT NULL,
 create_date      date NOT NULL,
 modified_user    string,
 modified_date    date
);
