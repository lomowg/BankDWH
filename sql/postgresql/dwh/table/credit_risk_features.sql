CREATE TABLE dwh.credit_risk_features
(
    id bigserial PRIMARY KEY,
    client_id bigint NOT NULL REFERENCES dwh.clients (client_id),
    source_customer_id varchar(32) NOT NULL,
    limit_balance numeric(18, 2),
    sex varchar(8),
    education varchar(32),
    marriage varchar(32),
    age int,
    pay_status_m1 int,
    pay_status_m2 int,
    bill_amt_m1 numeric(18, 2),
    bill_amt_m2 numeric(18, 2),
    pay_amt_m1 numeric(18, 2),
    pay_amt_m2 numeric(18, 2),
    default_next_month smallint,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_credit_risk_client UNIQUE (client_id)
);
