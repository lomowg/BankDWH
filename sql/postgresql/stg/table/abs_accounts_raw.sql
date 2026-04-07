CREATE TABLE stg.abs_accounts_raw
(
    id bigserial PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES stg.load_batch (batch_id),
    source_account_id varchar(100) NOT NULL,
    source_client_id varchar(100) NOT NULL,
    account_number_masked varchar(34),
    account_type varchar(50),
    currency_code varchar(8),
    opened_at varchar(32),
    closed_at varchar(32),
    status varchar(30),
    loaded_at timestamptz NOT NULL DEFAULT now()
);
