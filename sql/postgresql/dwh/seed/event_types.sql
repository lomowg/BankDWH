INSERT INTO dwh.event_types (event_type_id, event_type_code, event_type_name) VALUES
    (1, 'LOGIN', 'Вход'),
    (2, 'LOGOUT', 'Выход'),
    (3, 'SCREEN_VIEW', 'Просмотр экрана'),
    (4, 'PAYMENT_INIT', 'Инициация платежа')
ON CONFLICT (event_type_id) DO NOTHING;
