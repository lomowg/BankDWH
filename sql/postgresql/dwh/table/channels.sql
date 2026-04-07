CREATE TABLE dwh.channels
(
    channel_id smallint NOT NULL PRIMARY KEY,
    channel_code varchar(50) NOT NULL,
    channel_name varchar(100) NOT NULL,
    is_digital boolean NOT NULL
);
