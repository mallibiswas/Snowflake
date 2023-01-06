-------------------------------------------------------------------
----------------- Wifi Enriched Sightings Snowpipe
-------------------------------------------------------------------

create OR replace stage ZENPROD.PRESENCE.WIFI_ENRICHED_SIGHTINGS_S3_STAGE
    file_format = ( format_name = 'ZENPROD.PRESENCE.S3_PARQUET_FORMAT' )
    url = 's3://zp-uw2-foundation-kafka-archives/secor/presence_wifi_enrichedsighting_bycontactidlocationid/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-wifi-enrichedsighting-snowflake-stage');

create or replace table ZENPROD.PRESENCE.WIFI_ENRICHED_SIGHTINGS (
    SIGHTING_ID VARCHAR(16777216),
    STATUS VARCHAR(16777216),

    START_TIME TIMESTAMP,
    END_TIME TIMESTAMP,

    BLIP_COUNT NUMBER(38,0),
    MAX_RSSI INTEGER,
    MIN_RSSI INTEGER,
    AVG_RSSI FLOAT,

    CLIENT_MAC_INFO ARRAY,

    CONTACT_ID VARCHAR(16777216),
    CONTACT_INFO VARCHAR(16777216),
    CONTACT_METHOD VARCHAR(16777216),

    LOCATION_ID VARCHAR(16777216),
    ACCOUNT_ID VARCHAR(16777216),

    KNOWN_TO_ZENREACH BOOLEAN,
    KNOWN_TO_MERCHANT_ACCOUNT BOOLEAN,
    KNOWN_TO_MERCHANT_LOCATION BOOLEAN,

    PRIVACY_VERSION VARCHAR(16777216),
    TERMS_VERSION VARCHAR(16777216),
    BUNDLE_VERSION VARCHAR(16777216),

    IS_EMPLOYEE BOOLEAN,

    PORTAL_BLIP_COUNT NUMBER(38,0)
);

create or replace pipe ZENPROD.PRESENCE.WIFI_ENRICHED_SIGHTINGS_SNOWPIPE auto_ingest=true as
COPY INTO ZENPROD.PRESENCE.WIFI_ENRICHED_SIGHTINGS
FROM (SELECT 
    $1:sighting_id::string as sighting_id,
    nvl($1:status,'Status_UNKNOWN')::string as status,

    to_timestamp_ntz($1:start_time:seconds::integer * 1000 + nvl($1:start_time:nanos, 0)::integer / 1000000, 3) as start_time,
    to_timestamp_ntz($1:end_time:seconds::integer * 1000 + nvl($1:end_time:nanos, 0)::integer / 1000000, 3) as end_time,

    nvl($1:stats:blip_count, 0)::integer as blip_count,
    nvl($1:stats:max_rssi, 0)::integer as max_rssi,
    nvl($1:stats:min_rssi, 0)::integer as min_rssi,
    nvl($1:stats:avg_rssi, 0)::float as avg_rssi,
    nvl($1:stats:portal_blip_count, 0)::integer as portal_blip_count,

    $1:client_mac_info::array as client_mac_info,

    $1:contact_info:contact_id::string as contact_id,
    $1:contact_info:contact_info::string as contact_info,
    $1:contact_info:contact_method::string as contact_method,

    lower($1:location_info:location_id::string) as location_id,
    lower($1:location_info:account_id::string) as account_id,

    nvl($1:consent_info:known_to_zenreach, false)::boolean as known_to_zenreach,
    nvl($1:consent_info:known_to_merchant_account, false)::boolean as known_to_merchant_account,
    nvl($1:consent_info:known_to_merchant_location, false)::boolean as known_to_merchant_location,

    $1:consent_info:tos_version:privacy_version::string as privacy_version,
    $1:consent_info:tos_version:terms_version::string as terms_version,
    $1:consent_info:tos_version:bundle_version::string as bundle_version,

    nvl($1:filter_tags:is_employee, false)::boolean as is_employee,
    nvl($1:stats:portal_blip_count, 0)::integer as portal_blip_count

FROM @ZENPROD.PRESENCE.WIFI_ENRICHED_SIGHTINGS_S3_STAGE)
on_error = 'continue';
