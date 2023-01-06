-------------------------------------------------------------------
----------------- Meraki requests Using The New Archiver
-------------------------------------------------------------------

create OR replace stage ZENPROD.PRESENCE.MERAKI_REQUESTS_S3_STAGE
    file_format = ( TYPE = JSON )
    url = 's3://zp-uw2-foundation-kafka-archives/archiver/location_merakirequests_byapmac/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-archiver-meraki-requests-snowflake-stage');

-- Table will hold archiver data from our snowpipe
create table if not exists ZENPROD.PRESENCE.MERAKI_REQUESTS (
    INSERT_ID number AUTOINCREMENT,
    REQUEST_BODY VARCHAR(16777216),
    VERSION VARCHAR(16777216),
    SERVER_TIME TIMESTAMP,
    AP_MAC VARCHAR(16777216),
    ASOF_DATE DATE
);

-- Pipe will insert data from stage to our archiver table
create pipe if not exists ZENPROD.PRESENCE.MERAKI_REQUESTS_SNOWPIPE auto_ingest = true as
COPY INTO ZENPROD.PRESENCE.MERAKI_REQUESTS (request_body, version, server_time, ap_mac, asof_date)
FROM (SELECT
    $1:request_body::string as request_body,
    $1:version::string as version,
    TO_TIMESTAMP_NTZ($1:server_time) as server_time,
    HEX_ENCODE(BASE64_DECODE_BINARY($1:ap_mac::string)) as ap_mac,
    current_date as asof_date
FROM @ZENPROD.PRESENCE.MERAKI_REQUESTS_S3_STAGE)
on_error = 'continue';
