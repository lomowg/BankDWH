CREATE DICTIONARY IF NOT EXISTS bank_marts.dict_segment_type
(
    segment_type_id UInt16,
    segment_type_code String,
    segment_type_name String
)
PRIMARY KEY segment_type_id
SOURCE(CLICKHOUSE(
    DB 'bank_marts'
    TABLE 'dim_segment_type'
))
LAYOUT(FLAT())
LIFETIME(MIN 60 MAX 300);
