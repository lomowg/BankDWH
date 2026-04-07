CREATE INDEX idx_transactions_client_date ON dwh.transactions (client_id, operation_date DESC);
