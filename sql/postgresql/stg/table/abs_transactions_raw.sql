CREATE TABLE stg.abs_transactions_raw
(
    id bigserial PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES stg.load_batch (batch_id),
    source_transaction_id varchar(100) NOT NULL,
    source_client_id varchar(100) NOT NULL,
    source_account_id varchar(100),
    source_product_id varchar(100),
    channel_code varchar(50),
    operation_type_code varchar(50),
    operation_ts varchar(64),
    amount varchar(32),
    currency_code varchar(8),
    direction char(1),
    status varchar(30),
    merchant_name varchar(255),
    description text,
    loaded_at timestamptz NOT NULL DEFAULT now()
);
