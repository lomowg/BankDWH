CREATE TABLE stg.crm_clients_raw
(
    id bigserial PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES stg.load_batch (batch_id),
    source_client_id varchar(100) NOT NULL,
    tax_id varchar(20),
    phone varchar(30),
    email varchar(255),
    region_code varchar(20),
    loyalty_tier varchar(50),
    loaded_at timestamptz NOT NULL DEFAULT now()
);
