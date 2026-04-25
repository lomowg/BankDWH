CREATE INDEX IF NOT EXISTS idx_stg_product_catalog_batch
    ON stg.product_catalog_raw (batch_id);
