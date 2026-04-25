CREATE TABLE dwh.crm_marketing_leads
(
    id bigserial PRIMARY KEY,
    client_id bigint NOT NULL REFERENCES dwh.clients (client_id),
    campaign_id varchar(32) NOT NULL,
    age int,
    job varchar(64),
    marital varchar(32),
    education varchar(32),
    default_flag varchar(16),
    housing_loan varchar(16),
    personal_loan varchar(16),
    contact_channel varchar(32),
    last_contact_date date,
    campaign_contacts int,
    previous_outcome varchar(32),
    term_deposit_subscribed varchar(8),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_crm_lead_client_campaign UNIQUE (client_id, campaign_id)
);
