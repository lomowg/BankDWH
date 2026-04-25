CREATE TABLE stg.crm_marketing_raw
(
    id bigserial PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES stg.load_batch (batch_id),
    campaign_id varchar(32),
    customer_id varchar(32),
    age varchar(16),
    job varchar(64),
    marital varchar(32),
    education varchar(32),
    default_flag varchar(16),
    housing_loan varchar(16),
    personal_loan varchar(16),
    contact_channel varchar(32),
    last_contact_date varchar(32),
    campaign_contacts varchar(16),
    previous_outcome varchar(32),
    term_deposit_subscribed varchar(8),
    loaded_at timestamptz NOT NULL DEFAULT now()
);
