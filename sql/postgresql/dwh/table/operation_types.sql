CREATE TABLE dwh.operation_types
(
    operation_type_id smallint NOT NULL PRIMARY KEY,
    operation_type_code varchar(50) NOT NULL,
    operation_type_name varchar(150) NOT NULL
);
