-- Витрина «Профиль клиента»: показатели на отчётную дату (OLAP-слой)
CREATE TABLE IF NOT EXISTS bank_marts.mart_client_profile
(
    report_date Date COMMENT 'Отчётная дата',
    client_id UInt64 COMMENT 'Внутренний идентификатор клиента',
    unified_client_key UUID COMMENT 'Сквозной ключ клиента',
    client_type LowCardinality(String) COMMENT 'Тип клиента (физ./юр. и т.д.)',
    status LowCardinality(String) COMMENT 'Статус клиента',
    region_code Nullable(String) COMMENT 'Регион',
    active_accounts_cnt UInt32 COMMENT 'Число активных счетов',
    active_products_cnt UInt32 COMMENT 'Число активных продуктов',
    debit_turnover_30d Decimal(18, 2) COMMENT 'Сумма исходящих операций за 30 дней',
    credit_turnover_30d Decimal(18, 2) COMMENT 'Сумма входящих операций за 30 дней',
    operations_cnt_30d UInt32 COMMENT 'Количество операций за 30 дней',
    digital_events_cnt_30d UInt32 COMMENT 'Цифровые события за 30 дней',
    appeals_cnt_90d UInt32 COMMENT 'Обращения за 90 дней',
    last_transaction_ts Nullable(DateTime64(3, 'UTC')) COMMENT 'Время последней операции',
    last_digital_event_ts Nullable(DateTime64(3, 'UTC')) COMMENT 'Время последнего цифрового события',
    current_segment_type_id Nullable(UInt16) COMMENT 'Текущий сегмент (id типа сегмента)',
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT 'Техническая метка загрузки в витрину'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, client_id)
SETTINGS index_granularity = 8192;
