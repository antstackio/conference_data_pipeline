-- ************************** SqlDBM: Snowflake *************************
-- ************************* Generated by SqlDBM ************************


-- ************************************** conformed.session_poll_fact
CREATE TABLE conformed.session_poll_fact
(
 question_id             bigint NOT NULL,
 session_id              bigint NOT NULL,
 session_title           string NOT NULL,
 question_response_count int NOT NULL,
 create_date             date NOT NULL,
 create_user             string NOT NULL
);