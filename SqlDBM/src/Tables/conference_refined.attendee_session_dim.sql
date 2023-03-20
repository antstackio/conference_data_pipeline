-- **********************************************************************
-- ************************* Generated by SqlDBM ************************



-- ************************************** conference_refined.attendee_session_dim

CREATE TABLE IF NOT EXISTS conference_refined.attendee_session_dim
(
 session_id    bigint NOT NULL CONSTRAINT session_id_fk REFERENCES conference_refined.session_dim,
 attendee_id   bigint NOT NULL CONSTRAINT attendee_id_1_fk REFERENCES conference_refined.event_registrant_dim,
 attendee_type string NOT NULL,
 create_date   date NOT NULL,
 create_user   string NOT NULL,
 session_name  string NOT NULL
);