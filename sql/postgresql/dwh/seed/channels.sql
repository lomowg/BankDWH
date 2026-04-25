INSERT INTO dwh.channels (channel_id, channel_code, channel_name, is_digital) VALUES
    (1, 'BRANCH', 'Отделение', false),
    (2, 'ATM', 'Банкомат', false),
    (3, 'IB', 'Интернет-банк', true),
    (4, 'MB', 'Мобильный банк', true),
    (5, 'CC', 'Контакт-центр', false),
    (6, 'WEB_BANK', 'Веб-банк (DBO / Open API)', true),
    (7, 'MOBILE_APP', 'Мобильное приложение', true),
    (8, 'API', 'Программный интерфейс', true),
    (9, 'EMAIL', 'Электронная почта', true),
    (10, 'TELEPHONE', 'Телефон', false),
    (11, 'WEB', 'Веб-форма', true),
    (12, 'REFERRAL', 'Рекомендация', false),
    (13, 'POS', 'Терминал (POS)', false)
ON CONFLICT (channel_id) DO NOTHING;
