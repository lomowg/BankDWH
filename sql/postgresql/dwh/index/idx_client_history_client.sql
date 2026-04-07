CREATE INDEX idx_client_history_client ON dwh.client_history (client_id) WHERE is_current = true;
