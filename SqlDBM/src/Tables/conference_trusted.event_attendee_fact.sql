-- **********************************************************************
-- ************************* Generated by SqlDBM ************************



-- ************************************** conference_trusted.event_attendee_fact

CREATE TABLE IF NOT EXISTS conference_trusted.event_attendee_fact
(
 event_id         bigint NOT NULL COMMENT 'SK of the event dimension',
 event_name       string NOT NULL COMMENT 'Name of the event or conference',
 registrant_count int NOT NULL COMMENT 'Total no of people who registered for the event',
 attendee_count   int NOT NULL COMMENT 'Total no of people who attended the event',
 create_date      DATE NOT NULL,
 create_user      STRING NOT NULL
) COMMENT 'Fact table to store fact related to attendee, registrant of an event';
