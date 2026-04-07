-- Staging: сырые выгрузки, близкие к источникам (АБС, CRM, ДБО)
CREATE SCHEMA IF NOT EXISTS stg;

COMMENT ON SCHEMA stg IS 'Первичная загрузка (staging), формат близок к выгрузкам из источников';
