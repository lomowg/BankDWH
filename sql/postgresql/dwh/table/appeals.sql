CREATE TABLE dwh.appeals
(
    appeal_id bigint NOT NULL PRIMARY KEY,
    source_system_id smallint NOT NULL REFERENCES dwh.source_systems (source_system_id),
    source_appeal_id varchar(100) NOT NULL,
    client_id bigint NOT NULL REFERENCES dwh.clients (client_id),
    channel_id smallint REFERENCES dwh.channels (channel_id),
    appeal_type_id smallint REFERENCES dwh.appeal_types (appeal_type_id),
    created_ts timestamptz NOT NULL,
    resolved_ts timestamptz,
    status varchar(30) NOT NULL,
    priority smallint,
    result_text text,
    created_at timestamptz NOT NULL,
    CONSTRAINT uq_appeals_source UNIQUE (source_system_id, source_appeal_id)
);
