CREATE TABLE dwh.appeal_types
(
    appeal_type_id smallint NOT NULL PRIMARY KEY,
    appeal_type_code varchar(50) NOT NULL,
    appeal_type_name varchar(150) NOT NULL
);
