-- Staging: сырые выгрузки, близкие к источникам (АБС, CRM, ДБО)

CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE stg.load_batch
(
    batch_id bigserial PRIMARY KEY,
    source_system_code varchar(50),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status varchar(30) NOT NULL DEFAULT 'RUNNING',
    notes text
);

CREATE TABLE stg.abs_clients_raw
(
    id bigserial PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES stg.load_batch (batch_id),
    source_client_id varchar(100) NOT NULL,
    client_type varchar(20),
    first_name varchar(100),
    last_name varchar(100),
    middle_name varchar(100),
    birth_date varchar(32),
    registration_date varchar(32),
    tax_id varchar(20),
    phone varchar(30),
    email varchar(255),
    region_code varchar(20),
    status varchar(30),
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stg.crm_clients_raw
(
    id bigserial PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES stg.load_batch (batch_id),
    source_client_id varchar(100) NOT NULL,
    tax_id varchar(20),
    phone varchar(30),
    email varchar(255),
    region_code varchar(20),
    loyalty_tier varchar(50),
    loaded_at timestamptz NOT NULL DEFAULT now()
);

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

CREATE TABLE stg.dbo_events_raw
(
    id bigserial PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES stg.load_batch (batch_id),
    source_event_id varchar(100) NOT NULL,
    source_client_id varchar(100) NOT NULL,
    channel_code varchar(50) NOT NULL,
    event_type_code varchar(50) NOT NULL,
    event_ts varchar(64),
    device_type varchar(50),
    platform varchar(50),
    session_id varchar(100),
    success_flag varchar(10),
    payload_json text,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stg.appeals_raw
(
    id bigserial PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES stg.load_batch (batch_id),
    source_appeal_id varchar(100) NOT NULL,
    source_client_id varchar(100) NOT NULL,
    channel_code varchar(50),
    appeal_type_code varchar(50),
    created_ts varchar(64),
    resolved_ts varchar(64),
    status varchar(30),
    priority varchar(16),
    result_text text,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_stg_abs_clients_batch ON stg.abs_clients_raw (batch_id);
CREATE INDEX idx_stg_crm_clients_batch ON stg.crm_clients_raw (batch_id);
CREATE INDEX idx_stg_abs_accounts_batch ON stg.abs_accounts_raw (batch_id);
CREATE INDEX idx_stg_abs_products_batch ON stg.abs_products_raw (batch_id);
CREATE INDEX idx_stg_abs_tx_batch ON stg.abs_transactions_raw (batch_id);
CREATE INDEX idx_stg_dbo_events_batch ON stg.dbo_events_raw (batch_id);
CREATE INDEX idx_stg_appeals_batch ON stg.appeals_raw (batch_id);

COMMENT ON SCHEMA stg IS 'Первичная загрузка (staging), формат близок к выгрузкам из источников';
