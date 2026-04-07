CREATE TABLE dwh.digital_events
(
    digital_event_id bigint NOT NULL PRIMARY KEY,
    source_system_id smallint NOT NULL REFERENCES dwh.source_systems (source_system_id),
    source_event_id varchar(100) NOT NULL,
    client_id bigint NOT NULL REFERENCES dwh.clients (client_id),
    channel_id smallint NOT NULL REFERENCES dwh.channels (channel_id),
    event_type_id smallint NOT NULL REFERENCES dwh.event_types (event_type_id),
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
