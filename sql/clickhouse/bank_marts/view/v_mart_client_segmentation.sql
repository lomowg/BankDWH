-- Витрина сегментации: mart_client_profile + атрибуты сегмента
CREATE OR REPLACE VIEW bank_marts.v_mart_client_segmentation AS
SELECT
    p.report_date,
    p.client_id,
    p.unified_client_key,
    p.client_type,
    p.status,
    p.region_code,
    p.active_accounts_cnt,
    p.active_products_cnt,
    p.debit_turnover_30d,
    p.credit_turnover_30d,
    p.operations_cnt_30d,
    p.digital_events_cnt_30d,
    p.appeals_cnt_90d,
    p.last_transaction_ts,
    p.last_digital_event_ts,
    p.current_segment_type_id,
    dictGetOrDefault(
        'bank_marts.dict_segment_type',
        'segment_type_code',
        toUInt64(ifNull(p.current_segment_type_id, toUInt16(0))),
        'UNKNOWN'
    ) AS segment_type_code,
    dictGetOrDefault(
        'bank_marts.dict_segment_type',
        'segment_type_name',
        toUInt64(ifNull(p.current_segment_type_id, toUInt16(0))),
        'Не задан'
    ) AS segment_type_name,
    p.loaded_at
FROM bank_marts.mart_client_profile AS p;
