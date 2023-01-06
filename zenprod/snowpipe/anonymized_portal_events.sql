-------------------------------------------------------------------
----------------- Anonymized Portal Events Snowpipe
-------------------------------------------------------------------

create OR replace stage ZENPROD.PRESENCE.PORTAL_EVENTS_S3_STAGE
    file_format = ( format_name = 'ZENPROD.PRESENCE.S3_PARQUET_FORMAT' )
    url = 's3://zp-uw2-foundation-kafka-archives/secor/anonymizedportalevents_anonymizedportalevent_byanonymizedmac_0/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-anonymizedportalevents-snowflake-stage');

create or replace table ZENPROD.PRESENCE.PORTAL_EVENTS (
    ID VARCHAR(16777216),
    LOCATION_ID VARCHAR(16777216),
    SESSION_ID VARCHAR(16777216),
    AP_MAC VARCHAR(16777216),
    CLIENT_MAC_ANONYMIZATION VARCHAR(16777216),
    CREATED DATETIME,
    EVENT_TYPE VARCHAR(16777216),
    EVENT_CONTEXT VARCHAR(16777216),
    PLATFORM VARCHAR(16777216),
    BROWSER VARCHAR(16777216),
    BROWSER_VERSION VARCHAR(16777216),
    USER_AGENT VARCHAR(16777216),
    VENDOR_PREFIX VARCHAR(16777216)
);

create or replace pipe ZENPROD.PRESENCE.PORTAL_EVENTS_SNOWPIPE auto_ingest=true as
COPY INTO ZENPROD.PRESENCE.PORTAL_EVENTS
FROM (SELECT $1:id::string as id,
    lower($1:location_id::string) as location_id,
    $1:session_id::string as session_id,
    $1:ap_mac::string as ap_mac,
    lower($1:client_mac_anonymization::string) as client_mac_anonymization,
    $1:ap_type::string as ap_type,
    $1:timestamp:seconds::timestamp as created,
    $1:event_type::string as event_type,
    $1:event_context::string as event_context,
    $1:platform::string as platform,
    $1:browser::string as browser,
    $1:browser_version::string as browser_version,
    $1:user_agent::string as user_agent,
    $1:vendor_prefix::string as vendor_prefix
FROM @ZENPROD.PRESENCE.PORTAL_EVENTS_S3_STAGE)
on_error = 'continue';
