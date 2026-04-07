CREATE TABLE dwh.client_segments_history
(
    segment_history_id bigint NOT NULL PRIMARY KEY,
    client_id bigint NOT NULL REFERENCES dwh.clients (client_id),
    segment_type_id smallint NOT NULL REFERENCES dwh.segment_types (segment_type_id),
    score numeric(10, 4),
    effective_from date NOT NULL,
    effective_to date,
    is_current boolean NOT NULL,
    assigned_by varchar(50) NOT NULL
);
