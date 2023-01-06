-------------------------------------------------------------------
----------------- Anonymized Blips Snowpipe Using The New Archiver
-------------------------------------------------------------------

create OR replace stage ZENPROD.PRESENCE.ARCHIVER_ANONYMIZED_BLIPS_S3_STAGE
    file_format = ( TYPE = JSON )
    url = 's3://zp-uw2-foundation-kafka-archives/archiver/anonymizedblips_anonymizedblip_byclientmac_0/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-archiver-anonymizedblips-snowflake-stage');

-- Table will hold archiver data from our snowpipe
create table if not exists ZENPROD.PRESENCE.ARCHIVER_ANONYMIZED_BLIPS (
    SENSOR_TYPE VARCHAR(16777216),
    SENSOR_MAC VARCHAR(16777216),
    CLIENT_MAC_ANONYMIZATION VARCHAR(16777216),
    SERVER_TIME TIMESTAMP,
    TS TIMESTAMP,
    VALUE FLOAT,
    MAC_PREFIX VARCHAR(6),
    VENDOR_NAME VARCHAR(16777216),
    NON_HUMAN BOOLEAN
);

-- Pipe will insert data from stage to our archiver table
create pipe if not exists ZENPROD.PRESENCE.ARCHIVER_ANONYMIZED_BLIPS_SNOWPIPE auto_ingest = true as 
COPY INTO ZENPROD.PRESENCE.ARCHIVER_ANONYMIZED_BLIPS
FROM (SELECT 
    $1:sensor_type::string as sensor_type,

    HEX_ENCODE(BASE64_DECODE_BINARY($1:sensor_mac::string)) as sensor_mac,
    HEX_ENCODE(BASE64_DECODE_BINARY($1:client_mac_anonymization::string)) as client_mac_anonymization,

    TO_TIMESTAMP_NTZ($1:server_time) as server_time,
    TO_TIMESTAMP_NTZ($1:ts) as ts,

    $1:value::float as value,

    HEX_ENCODE(BASE64_DECODE_BINARY($1:vendor_info:mac_prefix::string)) as mac_prefix,
    nvl($1:vendor_info:vendor_name::string,'') as vendor_name,
    nvl($1:vendor_info:non_human, false)::boolean as non_human
    
FROM @ZENPROD.PRESENCE.ARCHIVER_ANONYMIZED_BLIPS_S3_STAGE)
on_error = 'continue';


