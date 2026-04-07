INSERT INTO dwh.operation_types (operation_type_id, operation_type_code, operation_type_name) VALUES
    (1, 'PAYMENT', 'Платёж'),
    (2, 'TRANSFER', 'Перевод'),
    (3, 'CASH_IN', 'Пополнение'),
    (4, 'CASH_OUT', 'Снятие'),
    (5, 'POS', 'Оплата картой')
ON CONFLICT (operation_type_id) DO NOTHING;
