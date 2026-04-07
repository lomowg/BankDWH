CREATE TABLE dwh.segment_types
(
    segment_type_id smallint NOT NULL PRIMARY KEY,
    segment_type_code varchar(50) NOT NULL,
    segment_type_name varchar(150) NOT NULL,
    description text
);
