-------------------------------------------------------------------
----------------- Wifi Enriched Sightings Snowpipe Using The New Archiver
-------------------------------------------------------------------

create OR replace stage ZENSTAG.PRESENCE.ARCHIVER_WIFI_ENRICHED_SIGHTINGS_S3_STAGE
    file_format = ( TYPE = JSON )
    url = 's3://zs-uw2-platform-kafka-archives/archiver/presence_wifi_enrichedsighting_bycontactidlocationid/'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zs-uw2-archiver-wifi-enrichedsighting-snowflake-stage');

create or replace table ZENSTAG.PRESENCE.ARCHIVER_WIFI_ENRICHED_SIGHTINGS (
    SIGHTING_ID VARCHAR(16777216),
    STATUS VARCHAR(16777216),

    START_TIME TIMESTAMP,
    END_TIME TIMESTAMP,

    BLIP_COUNT NUMBER(38,0),
    PORTAL_BLIP_COUNT NUMBER(38,0),
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

    IS_EMPLOYEE BOOLEAN
);


create or replace pipe ZENSTAG.PRESENCE.ARCHIVER_WIFI_ENRICHED_SIGHTINGS_SNOWPIPE auto_ingest=true as
COPY INTO ZENSTAG.PRESENCE.ARCHIVER_WIFI_ENRICHED_SIGHTINGS
FROM (SELECT 
    HEX_ENCODE(BASE64_DECODE_BINARY($1:sighting_id)) as sighting_id,
    nvl($1:status,'Status_UNKNOWN')::string as status,

    TO_TIMESTAMP_NTZ($1:start_time) as start_time,
    TO_TIMESTAMP_NTZ($1:end_time) as end_time,

    nvl($1:stats:blip_count, 0)::integer as blip_count,
    nvl($1:stats:portal_blip_count, 0)::integer as portal_blip_count,
    nvl($1:stats:max_rssi, 0)::integer as max_rssi,
    nvl($1:stats:min_rssi, 0)::integer as min_rssi,
    nvl($1:stats:avg_rssi, 0)::float as avg_rssi,

    DECODE_CLIENT_MAC_INFO($1:client_mac_info::array) as client_mac_info,

    HEX_ENCODE(BASE64_DECODE_BINARY($1:contact_info:contact_id)) as contact_id,
    $1:contact_info:contact_info::string as contact_info,
    $1:contact_info:contact_method::string as contact_method,

    HEX_ENCODE(BASE64_DECODE_BINARY($1:location_info:location_id)) as location_id,
    HEX_ENCODE(BASE64_DECODE_BINARY($1:location_info:account_id)) as account_id,

    nvl($1:consent_info:known_to_zenreach, false)::boolean as known_to_zenreach,
    nvl($1:consent_info:known_to_merchant_account, false)::boolean as known_to_merchant_account,
    nvl($1:consent_info:known_to_merchant_location, false)::boolean as known_to_merchant_location,

    nvl($1:consent_info:tos_version:privacy_version::string, '') as privacy_version,
    nvl($1:consent_info:tos_version:terms_version::string, '') as terms_version,
    nvl($1:consent_info:tos_version:bundle_version::string, '') as bundle_version,

    nvl($1:filter_tags:is_employee, false)::boolean as is_employee

FROM @ZENSTAG.PRESENCE.ARCHIVER_WIFI_ENRICHED_SIGHTINGS_S3_STAGE)
on_error = 'continue';
