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
