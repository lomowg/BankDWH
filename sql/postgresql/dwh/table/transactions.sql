CREATE TABLE dwh.transactions
(
    transaction_id bigint NOT NULL PRIMARY KEY,
    source_system_id smallint NOT NULL REFERENCES dwh.source_systems (source_system_id),
    source_transaction_id varchar(100) NOT NULL,
    client_id bigint NOT NULL REFERENCES dwh.clients (client_id),
    account_id bigint REFERENCES dwh.accounts (account_id),
    product_id bigint REFERENCES dwh.products (product_id),
    channel_id smallint REFERENCES dwh.channels (channel_id),
    operation_type_id smallint REFERENCES dwh.operation_types (operation_type_id),
    operation_ts timestamptz NOT NULL,
    operation_date date NOT NULL,
    amount numeric(18, 2) NOT NULL,
    currency_code char(3) NOT NULL,
    direction char(1) NOT NULL,
    status varchar(30),
    merchant_name varchar(255),
    description text,
    created_at timestamptz NOT NULL,
    CONSTRAINT uq_transactions_source UNIQUE (source_system_id, source_transaction_id)
);
