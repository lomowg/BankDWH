INSERT INTO dwh.event_types (event_type_id, event_type_code, event_type_name) VALUES
    (1, 'LOGIN', 'Вход'),
    (2, 'LOGOUT', 'Выход'),
    (3, 'SCREEN_VIEW', 'Просмотр экрана'),
    (4, 'PAYMENT_INIT', 'Инициация платежа'),
    (5, 'login_success', 'Успешный вход (DBO)'),
    (6, 'login_failed', 'Неуспешный вход (DBO)'),
    (7, 'card_limit_changed', 'Изменение лимита карты'),
    (8, 'balance_view', 'Просмотр баланса'),
    (9, 'support_chat_opened', 'Открыт чат поддержки'),
    (10, 'statement_download', 'Скачивание выписки'),
    (11, 'payment_created', 'Создание платежа'),
    (12, 'payment_confirmed', 'Подтверждение платежа')
ON CONFLICT (event_type_id) DO NOTHING;
