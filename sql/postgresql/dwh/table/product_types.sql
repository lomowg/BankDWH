CREATE TABLE dwh.product_types
(
    product_type_id smallint NOT NULL PRIMARY KEY,
    product_type_code varchar(50) NOT NULL,
    product_type_name varchar(100) NOT NULL
);
