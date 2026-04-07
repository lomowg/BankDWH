CREATE TABLE dwh.products
(
    product_id bigint NOT NULL PRIMARY KEY,
    source_system_id smallint NOT NULL REFERENCES dwh.source_systems (source_system_id),
    source_product_id varchar(100) NOT NULL,
    client_id bigint NOT NULL REFERENCES dwh.clients (client_id),
    account_id bigint REFERENCES dwh.accounts (account_id),
    product_type_id smallint NOT NULL REFERENCES dwh.product_types (product_type_id),
    product_name varchar(200),
    currency_code char(3),
    opened_at date,
    closed_at date,
    status varchar(30) NOT NULL,
    amount_limit numeric(18, 2),
    interest_rate numeric(9, 4),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT uq_products_source UNIQUE (source_system_id, source_product_id)
);
