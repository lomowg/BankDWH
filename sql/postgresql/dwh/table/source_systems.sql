CREATE TABLE dwh.source_systems
(
    source_system_id smallint NOT NULL PRIMARY KEY,
    source_system_code varchar(50) NOT NULL,
    source_system_name varchar(200) NOT NULL,
    description text
);
