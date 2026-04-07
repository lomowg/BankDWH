INSERT INTO dwh.channels (channel_id, channel_code, channel_name, is_digital) VALUES
    (1, 'BRANCH', 'Отделение', false),
    (2, 'ATM', 'Банкомат', false),
    (3, 'IB', 'Интернет-банк', true),
    (4, 'MB', 'Мобильный банк', true),
    (5, 'CC', 'Контакт-центр', false)
ON CONFLICT (channel_id) DO NOTHING;
