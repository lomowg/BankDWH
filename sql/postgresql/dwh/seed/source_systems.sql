INSERT INTO dwh.source_systems (source_system_id, source_system_code, source_system_name, description) VALUES
    (1, 'ABS', 'Автоматизированная банковская система', NULL),
    (2, 'CRM', 'CRM', NULL),
    (3, 'DBO', 'Дистанционное банковское обслуживание', NULL)
ON CONFLICT (source_system_id) DO NOTHING;
