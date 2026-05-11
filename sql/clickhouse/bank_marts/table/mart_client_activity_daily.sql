-- Витрина «Активность клиента»
CREATE TABLE IF NOT EXISTS bank_marts.mart_client_activity_daily
(
    activity_date Date COMMENT 'Календарный день' CODEC(LZ4),
    client_id UInt64 CODEC(LZ4),
    channel_code LowCardinality(String) COMMENT 'Канал (MB, IB, BRANCH, ...)' CODEC(LZ4),
    operations_cnt UInt32 COMMENT 'Число банковских операций' CODEC(ZSTD(3)),
    debit_amount Decimal(18, 2) COMMENT 'Сумма по дебету (исходящие)' CODEC(ZSTD(3)),
    credit_amount Decimal(18, 2) COMMENT 'Сумма по кредиту (входящие)' CODEC(ZSTD(3)),
    digital_events_cnt UInt32 COMMENT 'Число цифровых событий' CODEC(ZSTD(3)),
    digital_success_cnt UInt32 COMMENT 'Успешные цифровые события' CODEC(ZSTD(3)),
    digital_fail_cnt UInt32 COMMENT 'Неуспешные цифровые события' CODEC(ZSTD(3)),
    appeals_opened_cnt UInt32 COMMENT 'Новые обращения за день' CODEC(ZSTD(3)),
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(activity_date)
ORDER BY (client_id, activity_date, channel_code)
SETTINGS index_granularity = 8192;
