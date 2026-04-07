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
