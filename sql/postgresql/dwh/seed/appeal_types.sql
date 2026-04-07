INSERT INTO dwh.appeal_types (appeal_type_id, appeal_type_code, appeal_type_name) VALUES
    (1, 'SUPPORT', 'Обращение в поддержку'),
    (2, 'CLAIM', 'Жалоба'),
    (3, 'REQUEST', 'Заявка')
ON CONFLICT (appeal_type_id) DO NOTHING;
