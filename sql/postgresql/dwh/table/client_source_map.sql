CREATE TABLE dwh.client_source_map
(
    source_system_id smallint NOT NULL REFERENCES dwh.source_systems (source_system_id),
    source_client_id varchar(100) NOT NULL,
    client_id bigint NOT NULL REFERENCES dwh.clients (client_id),
    is_primary boolean NOT NULL,
    match_rule varchar(100),
    matched_at timestamptz NOT NULL,
    PRIMARY KEY (source_system_id, source_client_id)
);
