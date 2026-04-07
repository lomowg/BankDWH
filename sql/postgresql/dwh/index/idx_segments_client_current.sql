CREATE INDEX idx_segments_client_current ON dwh.client_segments_history (client_id) WHERE is_current = true;
