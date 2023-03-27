-- ************************** SqlDBM: Snowflake *************************
-- ************************* Generated by SqlDBM ************************


-- ************************************** conformed.event_dim
CREATE TABLE conformed.event_dim
(
 event_id      bigint NOT NULL,
 event_name    string NOT NULL,
 start_date    date NOT NULL,
 end_date      date NOT NULL,
 addr_line_1   string NOT NULL,
 addr_line_2   string NOT NULL,
 city          string NOT NULL,
 state         string NOT NULL,
 zipcode       string NOT NULL,
 country       string NOT NULL,
 create_date   date NOT NULL,
 create_user   string NOT NULL,
 modified_date string NOT NULL,
 modified_user string NOT NULL
);
