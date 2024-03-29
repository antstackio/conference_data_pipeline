-- **********************************************************************
-- ************************* Generated by SqlDBM ************************



-- ************************************** conference_refined.registrant

CREATE TABLE IF NOT EXISTS conference_refined.registrant
(
 attendee_id     bigint GENERATED BY DEFAULT AS IDENTITY COMMENT 'SK for attendee',
 session_title   string,
 attendee_type   string NOT NULL,
 registration_no string NOT NULL COMMENT 'Application generated registration no.',
 first_name      string NOT NULL COMMENT 'First name of the person',
 last_name       string NOT NULL COMMENT 'Last name of the person',
 job_role        string COMMENT 'Current job role of the person',
 state           string NOT NULL COMMENT 'State from which the person belongs',
 email_address   string NOT NULL COMMENT 'Email address of the person',
 logout_time     timestamp,
 login_time      timestamp,
 create_date     date NOT NULL COMMENT 'Creation date of the row',
 create_user     string NOT NULL COMMENT 'User who executed the sql query',
 modified_user   string COMMENT 'User who executed the sql query to modify the row',
 modified_date   STRING COMMENT 'Date when the raw modified',
 is_processed    boolean NOT NULL
) COMMENT 'Dimension table to people who registered for an event';
