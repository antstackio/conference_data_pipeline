-- ************************** SqlDBM: Snowflake *************************
-- ************************* Generated by SqlDBM ************************


-- ************************************** conformed.attendee_session_dim
CREATE TABLE conformed.attendee_session_dim
(
 session_id    string NOT NULL,
 session_title string NOT NULL,
 attendee_id   bigint NOT NULL,
 attendee_type string NOT NULL,
 create_date   date NOT NULL,
 create_user   string NOT NULL
);
