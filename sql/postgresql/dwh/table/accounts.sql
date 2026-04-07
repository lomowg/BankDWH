CREATE TABLE dwh.accounts
(
    account_id bigint NOT NULL PRIMARY KEY,
    source_system_id smallint NOT NULL REFERENCES dwh.source_systems (source_system_id),
    source_account_id varchar(100) NOT NULL,
    client_id bigint NOT NULL REFERENCES dwh.clients (client_id),
    account_number_masked varchar(34),
    account_type varchar(50),
    currency_code char(3),
    opened_at date,
    closed_at date,
    status varchar(30) NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT uq_accounts_source UNIQUE (source_system_id, source_account_id)
);
