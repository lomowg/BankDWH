-- Справочник типов сегментации (для словаря dict_segment_type и витрины-представления)
CREATE TABLE IF NOT EXISTS bank_marts.dim_segment_type
(
    segment_type_id UInt16,
    segment_type_code LowCardinality(String),
    segment_type_name String
)
ENGINE = MergeTree
ORDER BY segment_type_id;

INSERT INTO bank_marts.dim_segment_type (segment_type_id, segment_type_code, segment_type_name) VALUES
    (0, 'UNKNOWN', 'Не задан'),
    (1, 'ACTIVE', 'Активные клиенты (регулярно используют банковские услуги)'),
    (2, 'LOW_ACTIVITY', 'Малоактивные клиенты (редко используют услуги)'),
    (3, 'NEW', 'Новые клиенты'),
    (4, 'DORMANT', 'Неактивные клиенты (нет операций длительное время)'),
    (5, 'HIGH_VALUE', 'Потенциально ценные клиенты');
