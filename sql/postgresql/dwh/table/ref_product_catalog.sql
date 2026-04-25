CREATE TABLE dwh.ref_product_catalog
(
    product_id int NOT NULL PRIMARY KEY,
    product_name varchar(200),
    product_group varchar(50),
    description text,
    interest_rate numeric(12, 4),
    credit_limit numeric(18, 2),
    active_flag char(1),
    source_system varchar(64),
    created_at timestamptz NOT NULL DEFAULT now()
);
