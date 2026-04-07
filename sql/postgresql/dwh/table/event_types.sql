CREATE TABLE dwh.event_types
(
    event_type_id smallint NOT NULL PRIMARY KEY,
    event_type_code varchar(50) NOT NULL,
    event_type_name varchar(150) NOT NULL
);
