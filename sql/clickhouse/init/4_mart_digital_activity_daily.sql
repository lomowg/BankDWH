-- Детализация цифровой активности по типу события
CREATE TABLE IF NOT EXISTS bank_marts.mart_digital_activity_daily
(
    activity_date Date,
    client_id UInt64,
    channel_code LowCardinality(String),
    event_type_code LowCardinality(String),
    event_cnt UInt32,
    success_cnt UInt32,
    fail_cnt UInt32,
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(activity_date)
ORDER BY (client_id, activity_date, channel_code, event_type_code)
SETTINGS index_granularity = 8192;
