-- Сводные метрики по сегментам на отчётную дату
CREATE TABLE IF NOT EXISTS bank_marts.mart_segment_metrics
(
    report_date Date CODEC(LZ4),
    segment_type_id UInt16 CODEC(LZ4),
    clients_cnt UInt32 COMMENT 'Клиентов в сегменте на дату' CODEC(ZSTD(3)),
    active_clients_30d UInt32 COMMENT 'Активных за последние 30 дней от report_date' CODEC(ZSTD(3)),
    total_debit_turnover_30d Decimal(24, 2) CODEC(ZSTD(3)),
    total_credit_turnover_30d Decimal(24, 2) CODEC(ZSTD(3)),
    total_operations_30d UInt64 CODEC(ZSTD(3)),
    avg_operations_per_client Float32 CODEC(ZSTD(3)),
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, segment_type_id)
SETTINGS index_granularity = 8192;
