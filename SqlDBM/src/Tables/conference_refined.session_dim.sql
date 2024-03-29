-- **********************************************************************
-- ************************* Generated by SqlDBM ************************



-- ************************************** conference_refined.session_dim

CREATE TABLE IF NOT EXISTS conference_refined.session_dim
(
 session_id       bigint GENERATED BY DEFAULT AS IDENTITY CONSTRAINT session_id_pk PRIMARY KEY,
 event_id         bigint NOT NULL CONSTRAINT event_id_fk REFERENCES conference_refined.event_dim,
 session_title    string NOT NULL,
 speakers         string NOT NULL,
 supporter        string NOT NULL,
 event_name       string NOT NULL,
 session_date     date NOT NULL,
 start_time       timestamp NOT NULL,
 end_time         timestamp NOT NULL,
 exact_start_time timestamp,
 exact_end_time   timestamp,
 create_user      string,
 create_date      date,
 modified_user    string,
 modified_date    date
);
