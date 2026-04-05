-- Детализация финансовой активности по типу операции
CREATE TABLE IF NOT EXISTS bank_marts.mart_financial_activity_daily
(
    activity_date Date,
    client_id UInt64,
    channel_code LowCardinality(String),
    operation_type_code LowCardinality(String),
    operation_cnt UInt32,
    debit_amount Decimal(18, 2),
    credit_amount Decimal(18, 2),
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(activity_date)
ORDER BY (client_id, activity_date, channel_code, operation_type_code)
SETTINGS index_granularity = 8192;
