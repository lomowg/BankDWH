-- Ядро хранилища (PostgreSQL): схема dwh
CREATE SCHEMA IF NOT EXISTS dwh;

SET search_path TO dwh, public;

CREATE TABLE source_systems
(
    source_system_id smallint NOT NULL PRIMARY KEY,
    source_system_code varchar(50) NOT NULL,
    source_system_name varchar(200) NOT NULL,
    description text
);

CREATE TABLE channels
(
    channel_id smallint NOT NULL PRIMARY KEY,
    channel_code varchar(50) NOT NULL,
    channel_name varchar(100) NOT NULL,
    is_digital boolean NOT NULL
);

CREATE TABLE product_types
(
    product_type_id smallint NOT NULL PRIMARY KEY,
    product_type_code varchar(50) NOT NULL,
    product_type_name varchar(100) NOT NULL
);

CREATE TABLE operation_types
(
    operation_type_id smallint NOT NULL PRIMARY KEY,
    operation_type_code varchar(50) NOT NULL,
    operation_type_name varchar(150) NOT NULL
);

CREATE TABLE event_types
(
    event_type_id smallint NOT NULL PRIMARY KEY,
    event_type_code varchar(50) NOT NULL,
    event_type_name varchar(150) NOT NULL
);

CREATE TABLE appeal_types
(
    appeal_type_id smallint NOT NULL PRIMARY KEY,
    appeal_type_code varchar(50) NOT NULL,
    appeal_type_name varchar(150) NOT NULL
);

CREATE TABLE segment_types
(
    segment_type_id smallint NOT NULL PRIMARY KEY,
    segment_type_code varchar(50) NOT NULL,
    segment_type_name varchar(150) NOT NULL,
    description text
);

CREATE TABLE clients
(
    client_id bigint NOT NULL PRIMARY KEY,
    unified_client_key uuid NOT NULL,
    client_type varchar(20) NOT NULL,
    first_name varchar(100),
    last_name varchar(100),
    middle_name varchar(100),
    full_name varchar(300),
    birth_date date,
    registration_date date,
    tax_id varchar(20),
    phone varchar(30),
    email varchar(255),
    region_code varchar(20),
    status varchar(30) NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE client_source_map
(
    source_system_id smallint NOT NULL REFERENCES source_systems (source_system_id),
    source_client_id varchar(100) NOT NULL,
    client_id bigint NOT NULL REFERENCES clients (client_id),
    is_primary boolean NOT NULL,
    match_rule varchar(100),
    matched_at timestamptz NOT NULL,
    PRIMARY KEY (source_system_id, source_client_id)
);

CREATE TABLE client_history
(
    client_history_id bigint NOT NULL PRIMARY KEY,
    client_id bigint NOT NULL REFERENCES clients (client_id),
    full_name varchar(300),
    phone varchar(30),
    email varchar(255),
    region_code varchar(20),
    status varchar(30),
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    is_current boolean NOT NULL
);

CREATE TABLE accounts
(
    account_id bigint NOT NULL PRIMARY KEY,
    source_system_id smallint NOT NULL REFERENCES source_systems (source_system_id),
    source_account_id varchar(100) NOT NULL,
    client_id bigint NOT NULL REFERENCES clients (client_id),
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

CREATE TABLE products
(
    product_id bigint NOT NULL PRIMARY KEY,
    source_system_id smallint NOT NULL REFERENCES source_systems (source_system_id),
    source_product_id varchar(100) NOT NULL,
    client_id bigint NOT NULL REFERENCES clients (client_id),
    account_id bigint REFERENCES accounts (account_id),
    product_type_id smallint NOT NULL REFERENCES product_types (product_type_id),
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

CREATE TABLE transactions
(
    transaction_id bigint NOT NULL PRIMARY KEY,
    source_system_id smallint NOT NULL REFERENCES source_systems (source_system_id),
    source_transaction_id varchar(100) NOT NULL,
    client_id bigint NOT NULL REFERENCES clients (client_id),
    account_id bigint REFERENCES accounts (account_id),
    product_id bigint REFERENCES products (product_id),
    channel_id smallint REFERENCES channels (channel_id),
    operation_type_id smallint REFERENCES operation_types (operation_type_id),
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

CREATE TABLE digital_events
(
    digital_event_id bigint NOT NULL PRIMARY KEY,
    source_system_id smallint NOT NULL REFERENCES source_systems (source_system_id),
    source_event_id varchar(100) NOT NULL,
    client_id bigint NOT NULL REFERENCES clients (client_id),
    channel_id smallint NOT NULL REFERENCES channels (channel_id),
    event_type_id smallint NOT NULL REFERENCES event_types (event_type_id),
    event_ts timestamptz NOT NULL,
    event_date date NOT NULL,
    device_type varchar(50),
    platform varchar(50),
    session_id varchar(100),
    success_flag boolean,
    payload jsonb,
    created_at timestamptz NOT NULL,
    CONSTRAINT uq_digital_events_source UNIQUE (source_system_id, source_event_id)
);

CREATE TABLE appeals
(
    appeal_id bigint NOT NULL PRIMARY KEY,
    source_system_id smallint NOT NULL REFERENCES source_systems (source_system_id),
    source_appeal_id varchar(100) NOT NULL,
    client_id bigint NOT NULL REFERENCES clients (client_id),
    channel_id smallint REFERENCES channels (channel_id),
    appeal_type_id smallint REFERENCES appeal_types (appeal_type_id),
    created_ts timestamptz NOT NULL,
    resolved_ts timestamptz,
    status varchar(30) NOT NULL,
    priority smallint,
    result_text text,
    created_at timestamptz NOT NULL,
    CONSTRAINT uq_appeals_source UNIQUE (source_system_id, source_appeal_id)
);

CREATE TABLE client_segments_history
(
    segment_history_id bigint NOT NULL PRIMARY KEY,
    client_id bigint NOT NULL REFERENCES clients (client_id),
    segment_type_id smallint NOT NULL REFERENCES segment_types (segment_type_id),
    score numeric(10, 4),
    effective_from date NOT NULL,
    effective_to date,
    is_current boolean NOT NULL,
    assigned_by varchar(50) NOT NULL
);

CREATE TABLE client_profile_export
(
    report_date date NOT NULL,
    client_id bigint NOT NULL REFERENCES clients (client_id),
    unified_client_key uuid NOT NULL,
    client_type varchar(20) NOT NULL,
    status varchar(30) NOT NULL,
    region_code varchar(20),
    active_accounts_cnt int NOT NULL,
    active_products_cnt int NOT NULL,
    debit_turnover_30d numeric(18, 2) NOT NULL,
    credit_turnover_30d numeric(18, 2) NOT NULL,
    operations_cnt_30d int NOT NULL,
    digital_events_cnt_30d int NOT NULL,
    appeals_cnt_90d int NOT NULL,
    last_transaction_ts timestamptz,
    last_digital_event_ts timestamptz,
    current_segment_type_id smallint REFERENCES segment_types (segment_type_id),
    PRIMARY KEY (report_date, client_id)
);

CREATE INDEX idx_client_source_map_client ON client_source_map (client_id);
CREATE INDEX idx_client_history_client ON client_history (client_id) WHERE is_current = true;
CREATE INDEX idx_accounts_client ON accounts (client_id);
CREATE INDEX idx_products_client ON products (client_id);
CREATE INDEX idx_transactions_client_date ON transactions (client_id, operation_date DESC);
CREATE INDEX idx_transactions_op_ts ON transactions (operation_ts DESC);
CREATE INDEX idx_digital_events_client_date ON digital_events (client_id, event_date DESC);
CREATE INDEX idx_appeals_client ON appeals (client_id);
CREATE INDEX idx_segments_client_current ON client_segments_history (client_id) WHERE is_current = true;
CREATE INDEX idx_profile_export_report ON client_profile_export (report_date);

COMMENT ON SCHEMA dwh IS 'Ядро хранилища данных (клиент, продукты, операции, витрина экспорта профиля)';
