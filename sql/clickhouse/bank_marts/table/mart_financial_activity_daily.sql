-- Детализация финансовой активности по типу операции
CREATE TABLE IF NOT EXISTS bank_marts.mart_financial_activity_daily
(
    activity_date Date CODEC(LZ4),
    client_id UInt64 CODEC(LZ4),
    channel_code LowCardinality(String) CODEC(LZ4),
    operation_type_code LowCardinality(String) CODEC(LZ4),
    operation_cnt UInt32 CODEC(ZSTD(3)),
    debit_amount Decimal(18, 2) CODEC(ZSTD(3)),
    credit_amount Decimal(18, 2) CODEC(ZSTD(3)),
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(activity_date)
ORDER BY (client_id, activity_date, channel_code, operation_type_code)
SETTINGS index_granularity = 8192;
