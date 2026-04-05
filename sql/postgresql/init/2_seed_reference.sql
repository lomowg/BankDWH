SET search_path TO dwh, public;

INSERT INTO source_systems (source_system_id, source_system_code, source_system_name, description) VALUES
    (1, 'ABS', 'Автоматизированная банковская система', NULL),
    (2, 'CRM', 'CRM', NULL),
    (3, 'DBO', 'Дистанционное банковское обслуживание', NULL)
ON CONFLICT (source_system_id) DO NOTHING;

INSERT INTO channels (channel_id, channel_code, channel_name, is_digital) VALUES
    (1, 'BRANCH', 'Отделение', false),
    (2, 'ATM', 'Банкомат', false),
    (3, 'IB', 'Интернет-банк', true),
    (4, 'MB', 'Мобильный банк', true),
    (5, 'CC', 'Контакт-центр', false)
ON CONFLICT (channel_id) DO NOTHING;

INSERT INTO product_types (product_type_id, product_type_code, product_type_name) VALUES
    (1, 'CARD', 'Карта'),
    (2, 'ACCOUNT', 'Счёт'),
    (3, 'DEPOSIT', 'Депозит'),
    (4, 'LOAN', 'Кредит')
ON CONFLICT (product_type_id) DO NOTHING;

INSERT INTO operation_types (operation_type_id, operation_type_code, operation_type_name) VALUES
    (1, 'PAYMENT', 'Платёж'),
    (2, 'TRANSFER', 'Перевод'),
    (3, 'CASH_IN', 'Пополнение'),
    (4, 'CASH_OUT', 'Снятие'),
    (5, 'POS', 'Оплата картой')
ON CONFLICT (operation_type_id) DO NOTHING;

INSERT INTO event_types (event_type_id, event_type_code, event_type_name) VALUES
    (1, 'LOGIN', 'Вход'),
    (2, 'LOGOUT', 'Выход'),
    (3, 'SCREEN_VIEW', 'Просмотр экрана'),
    (4, 'PAYMENT_INIT', 'Инициация платежа')
ON CONFLICT (event_type_id) DO NOTHING;

INSERT INTO appeal_types (appeal_type_id, appeal_type_code, appeal_type_name) VALUES
    (1, 'SUPPORT', 'Обращение в поддержку'),
    (2, 'CLAIM', 'Жалоба'),
    (3, 'REQUEST', 'Заявка')
ON CONFLICT (appeal_type_id) DO NOTHING;

INSERT INTO segment_types (segment_type_id, segment_type_code, segment_type_name, description) VALUES
    (1, 'ACTIVE', 'Активный', NULL),
    (2, 'LOW_ACTIVITY', 'Малоактивный', NULL),
    (3, 'NEW', 'Новый', NULL),
    (4, 'DORMANT', 'Спящий', NULL)
ON CONFLICT (segment_type_id) DO NOTHING;
