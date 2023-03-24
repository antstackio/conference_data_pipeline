-- **********************************************************************
-- ************************* Generated by SqlDBM ************************



-- ************************************** conference_trusted.event_registrant_dim

CREATE TABLE IF NOT EXISTS conference_trusted.event_registrant_dim
(
 attendee_id     bigint GENERATED BY DEFAULT AS IDENTITY COMMENT 'SK for attendee',
 event_id        bigint NOT NULL,
 event_name      string NOT NULL COMMENT 'Name of the event',
 registration_no string COMMENT 'Application generated registration no.',
 first_name      string COMMENT 'First name of the person',
 last_name       string NOT NULL COMMENT 'Last name of the person',
 job_role        string NOT NULL COMMENT 'Current job role of the person',
 state           string NOT NULL COMMENT 'State from which the person belongs',
 email_address   string NOT NULL COMMENT 'Email address of the person',
 create_date     date COMMENT 'Creation date of the row',
 create_user     string COMMENT 'User who executed the sql query',
 modified_user   string COMMENT 'User who executed the sql query to modify the row',
 modified_date   date COMMENT 'Date when the raw modified'
) COMMENT 'Dimension table to people who registered for an event';