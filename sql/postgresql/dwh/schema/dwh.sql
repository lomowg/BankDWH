-- Ядро хранилища (PostgreSQL): схема dwh
CREATE SCHEMA IF NOT EXISTS dwh;

COMMENT ON SCHEMA dwh IS 'Ядро хранилища данных (клиент, продукты, операции, витрина экспорта профиля)';
