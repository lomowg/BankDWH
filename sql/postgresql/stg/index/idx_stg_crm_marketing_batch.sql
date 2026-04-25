CREATE INDEX IF NOT EXISTS idx_stg_crm_marketing_batch
    ON stg.crm_marketing_raw (batch_id);
