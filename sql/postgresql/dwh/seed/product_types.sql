INSERT INTO dwh.product_types (product_type_id, product_type_code, product_type_name) VALUES
    (1, 'CARD', 'Карта'),
    (2, 'ACCOUNT', 'Счёт'),
    (3, 'DEPOSIT', 'Депозит'),
    (4, 'LOAN', 'Кредит')
ON CONFLICT (product_type_id) DO NOTHING;
