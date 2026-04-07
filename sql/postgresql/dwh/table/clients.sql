CREATE TABLE dwh.clients
(
    client_id bigint NOT NULL PRIMARY KEY,
    unified_client_key uuid NOT NULL,
    client_type varchar(20) NOT NULL,
    first_name varchar(100),
    last_name varchar(100),
    middle_name varchar(100),
    full_name varchar(300),
    birth_date date,
    registration_date date,
    tax_id varchar(20),
    phone varchar(30),
    email varchar(255),
    region_code varchar(20),
    status varchar(30) NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);
