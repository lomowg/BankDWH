CREATE TABLE stg.credit_risk_raw
(
    id bigserial PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES stg.load_batch (batch_id),
    customer_id varchar(32) NOT NULL,
    limit_balance varchar(32),
    sex varchar(8),
    education varchar(32),
    marriage varchar(32),
    age varchar(16),
    pay_status_m1 varchar(8),
    pay_status_m2 varchar(8),
    bill_amt_m1 varchar(32),
    bill_amt_m2 varchar(32),
    pay_amt_m1 varchar(32),
    pay_amt_m2 varchar(32),
    default_next_month varchar(4),
    loaded_at timestamptz NOT NULL DEFAULT now()
);
