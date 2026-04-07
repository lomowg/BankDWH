CREATE TABLE stg.abs_clients_raw
(
    id bigserial PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES stg.load_batch (batch_id),
    source_client_id varchar(100) NOT NULL,
    client_type varchar(20),
    first_name varchar(100),
    last_name varchar(100),
    middle_name varchar(100),
    birth_date varchar(32),
    registration_date varchar(32),
    tax_id varchar(20),
    phone varchar(30),
    email varchar(255),
    region_code varchar(20),
    status varchar(30),
    loaded_at timestamptz NOT NULL DEFAULT now()
);
