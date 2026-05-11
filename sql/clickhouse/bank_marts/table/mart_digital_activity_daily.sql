-- Детализация цифровой активности по типу события
CREATE TABLE IF NOT EXISTS bank_marts.mart_digital_activity_daily
(
    activity_date Date CODEC(LZ4),
    client_id UInt64 CODEC(LZ4),
    channel_code LowCardinality(String) CODEC(LZ4),
    event_type_code LowCardinality(String) CODEC(LZ4),
    event_cnt UInt32 CODEC(ZSTD(3)),
    success_cnt UInt32 CODEC(ZSTD(3)),
    fail_cnt UInt32 CODEC(ZSTD(3)),
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(activity_date)
ORDER BY (client_id, activity_date, channel_code, event_type_code)
SETTINGS index_granularity = 8192;
