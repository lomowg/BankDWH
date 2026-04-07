INSERT INTO dwh.segment_types (segment_type_id, segment_type_code, segment_type_name, description) VALUES
    (1, 'ACTIVE', 'Активные (регулярное использование услуг)', NULL),
    (2, 'LOW_ACTIVITY', 'Малоактивные (редкое использование)', NULL),
    (3, 'NEW', 'Новые клиенты', NULL),
    (4, 'DORMANT', 'Неактивные (долго без операций)', NULL),
    (5, 'HIGH_VALUE', 'Потенциально ценные', NULL)
ON CONFLICT (segment_type_id) DO UPDATE SET
    segment_type_name = EXCLUDED.segment_type_name,
    description = EXCLUDED.description;
