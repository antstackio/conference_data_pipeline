-- ************************** SqlDBM: Snowflake *************************
-- ************************* Generated by SqlDBM ************************


-- ************************************** conformed.event_registrant_dim
CREATE TABLE conformed.event_registrant_dim
(
 attendee_id     bigint NOT NULL,
 event_id        bigint NOT NULL,
 event_name      string NOT NULL,
 registration_no string NOT NULL,
 first_name      string NOT NULL,
 last_name       string NOT NULL,
 job_role        string NOT NULL,
 state           string NOT NULL,
 email_address   string NOT NULL,
 create_date     date NOT NULL,
 create_user     string NOT NULL,
 modified_date   date NOT NULL,
 modified_user   string NOT NULL
);