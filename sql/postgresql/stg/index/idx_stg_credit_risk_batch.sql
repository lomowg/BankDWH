CREATE INDEX IF NOT EXISTS idx_stg_credit_risk_batch
    ON stg.credit_risk_raw (batch_id);
