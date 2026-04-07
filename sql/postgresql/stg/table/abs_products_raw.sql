CREATE TABLE stg.abs_products_raw
(
    id bigserial PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES stg.load_batch (batch_id),
    source_product_id varchar(100) NOT NULL,
    source_client_id varchar(100) NOT NULL,
    source_account_id varchar(100),
    product_type_code varchar(50),
    product_name varchar(200),
    currency_code varchar(8),
    opened_at varchar(32),
    closed_at varchar(32),
    status varchar(30),
    amount_limit varchar(32),
    interest_rate varchar(32),
    loaded_at timestamptz NOT NULL DEFAULT now()
);
