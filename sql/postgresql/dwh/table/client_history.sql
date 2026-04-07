CREATE TABLE dwh.client_history
(
    client_history_id bigint NOT NULL PRIMARY KEY,
    client_id bigint NOT NULL REFERENCES dwh.clients (client_id),
    full_name varchar(300),
    phone varchar(30),
    email varchar(255),
    region_code varchar(20),
    status varchar(30),
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    is_current boolean NOT NULL
);
