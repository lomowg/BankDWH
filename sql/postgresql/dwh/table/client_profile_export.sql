CREATE TABLE dwh.client_profile_export
(
    report_date date NOT NULL,
    client_id bigint NOT NULL REFERENCES dwh.clients (client_id),
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
    current_segment_type_id smallint REFERENCES dwh.segment_types (segment_type_id),
    PRIMARY KEY (report_date, client_id)
);
