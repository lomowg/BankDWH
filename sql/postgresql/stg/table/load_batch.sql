CREATE TABLE stg.load_batch
(
    batch_id bigserial PRIMARY KEY,
    source_system_code varchar(50),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status varchar(30) NOT NULL DEFAULT 'RUNNING',
    notes text
);
