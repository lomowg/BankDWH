CREATE TABLE stg.product_catalog_raw
(
    id bigserial PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES stg.load_batch (batch_id),
    product_id int NOT NULL,
    product_name varchar(200),
    product_group varchar(50),
    description text,
    interest_rate varchar(32),
    credit_limit varchar(32),
    active_flag varchar(4),
    source_system varchar(64),
    loaded_at timestamptz NOT NULL DEFAULT now()
);
