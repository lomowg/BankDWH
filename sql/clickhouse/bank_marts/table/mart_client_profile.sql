-- Витрина «Профиль клиента»
CREATE TABLE IF NOT EXISTS bank_marts.mart_client_profile
(
    report_date Date COMMENT 'Отчётная дата' CODEC(LZ4),
    client_id UInt64 COMMENT 'Внутренний идентификатор клиента' CODEC(LZ4),
    unified_client_key UUID COMMENT 'Сквозной ключ клиента' CODEC(LZ4),
    client_type LowCardinality(String) COMMENT 'Тип клиента (физ./юр. и т.д.)' CODEC(LZ4),
    status LowCardinality(String) COMMENT 'Статус клиента' CODEC(LZ4),
    region_code Nullable(String) COMMENT 'Регион' CODEC(ZSTD(3)),
    active_accounts_cnt UInt32 COMMENT 'Число активных счетов' CODEC(ZSTD(3)),
    active_products_cnt UInt32 COMMENT 'Число активных продуктов' CODEC(ZSTD(3)),
    debit_turnover_30d Decimal(18, 2) COMMENT 'Сумма исходящих операций за 30 дней' CODEC(ZSTD(3)),
    credit_turnover_30d Decimal(18, 2) COMMENT 'Сумма входящих операций за 30 дней' CODEC(ZSTD(3)),
    operations_cnt_30d UInt32 COMMENT 'Количество операций за 30 дней' CODEC(ZSTD(3)),
    digital_events_cnt_30d UInt32 COMMENT 'Цифровые события за 30 дней' CODEC(ZSTD(3)),
    appeals_cnt_90d UInt32 COMMENT 'Обращения за 90 дней' CODEC(ZSTD(3)),
    last_transaction_ts Nullable(DateTime64(3, 'UTC')) COMMENT 'Время последней операции' CODEC(ZSTD(3)),
    last_digital_event_ts Nullable(DateTime64(3, 'UTC')) COMMENT 'Время последнего цифрового события' CODEC(ZSTD(3)),
    current_segment_type_id Nullable(UInt16) COMMENT 'Текущий сегмент (id типа сегмента)' CODEC(LZ4),
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT 'Техническая метка загрузки в витрину' CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, client_id)
SETTINGS index_granularity = 8192;
