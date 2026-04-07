-- Витрина «Активность клиента»: ежедневные агрегаты по каналам
CREATE TABLE IF NOT EXISTS bank_marts.mart_client_activity_daily
(
    activity_date Date COMMENT 'Календарный день',
    client_id UInt64,
    channel_code LowCardinality(String) COMMENT 'Канал (MB, IB, BRANCH, ...)',
    operations_cnt UInt32 COMMENT 'Число банковских операций',
    debit_amount Decimal(18, 2) COMMENT 'Сумма по дебету (исходящие)',
    credit_amount Decimal(18, 2) COMMENT 'Сумма по кредиту (входящие)',
    digital_events_cnt UInt32 COMMENT 'Число цифровых событий',
    digital_success_cnt UInt32 COMMENT 'Успешные цифровые события',
    digital_fail_cnt UInt32 COMMENT 'Неуспешные цифровые события',
    appeals_opened_cnt UInt32 COMMENT 'Новые обращения за день',
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(activity_date)
ORDER BY (client_id, activity_date, channel_code)
SETTINGS index_granularity = 8192;
