-------------------------------------------------------------------
----------------- Enriched Blips Snowpipe Using The New Archiver
-------------------------------------------------------------------

create OR replace stage ZENPROD.PRESENCE.ARCHIVER_WIFI_ENRICHED_BLIPS_S3_STAGE
    file_format = ( TYPE = JSON )
    url = 's3://zp-uw2-foundation-kafka-archives/archiver/presence_wifi_enrichedblip_bylocationid/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-archiver-wifi-enrichedblips-snowflake-stage');

-- Table will hold archiver data from our snowpipe
create table if not exists ZENPROD.PRESENCE.ARCHIVER_WIFI_ENRICHED_BLIPS (
   AP_TYPE VARCHAR(16777216),
   AP_MAC VARCHAR(16777216),
   
   CLIENT_MAC VARCHAR(16777216),
   
   RSSI INTEGER,
   
   SERVER_TS TIMESTAMP,
   LOCATION_ID VARCHAR(16777216),
   ACCOUNT_ID VARCHAR(16777216),
   
   CONTACT_ID VARCHAR(16777216),
   CONTACT_METHOD VARCHAR(16777216),
   CONTACT_INFO VARCHAR(16777216),
   
   KNOWN_TO_ZENREACH BOOLEAN,
   KNOWN_TO_MERCHANT_ACCOUNT BOOLEAN,
   KNOWN_TO_MERCHANT_LOCATION BOOLEAN,
   
   PRIVACY_VERSION VARCHAR(16777216),
   TERMS_VERSION VARCHAR(16777216),
   BUNDLE_VERSION VARCHAR(16777216),
   
   CLIENT_MAC_ANONYMIZATION VARCHAR(16777216),
   
   MAC_PREFIX VARCHAR(6),
   VENDOR_NAME VARCHAR(16777216),
   NON_HUMAN BOOLEAN 
);

-- Pipe will insert data from stage to our archiver table
create pipe if not exists ZENPROD.PRESENCE.ARCHIVER_WIFI_ENRICHED_BLIPS_SNOWPIPE auto_ingest = true as 
COPY INTO ZENPROD.PRESENCE.ARCHIVER_WIFI_ENRICHED_BLIPS
FROM (SELECT 
    nvl($1:ap_type, 'APType_UNKNOWN')::string as ap_type,
    ZENPROD.PRESENCE.string_to_mac(lower($1:ap_mac::string)) as ap_mac,

    ZENPROD.PRESENCE.string_to_mac(lower($1:client_mac::string)) as client_mac,

    nvl($1:stats:rssi, 0)::integer as rssi,

    TO_TIMESTAMP_NTZ($1:server_ts) as server_ts,

    HEX_ENCODE(BASE64_DECODE_BINARY($1:location_id::string)) as location_id,
    HEX_ENCODE(BASE64_DECODE_BINARY($1:account_id::string)) as account_id,

    HEX_ENCODE(BASE64_DECODE_BINARY($1:contact_id::string)) as contact_id,
    $1:contact_method::string as contact_method,
    $1:contact_info::string as contact_info,

    nvl($1:known_to_zenreach, false)::boolean as known_to_zenreach,
    nvl($1:known_to_merchant_account, false)::boolean as known_to_merchant_account,
    nvl($1:known_to_merchant_location, false)::boolean as known_to_merchant_location,

    $1:tos_version:privacy_version::string as privacy_version,
    $1:tos_version:terms_version::string as terms_version,
    $1:tos_version:bundle_version::string as bundle_version,

    HEX_ENCODE(BASE64_DECODE_BINARY($1:client_mac_anonymization::string)) as client_mac_anonymization,

    HEX_ENCODE(BASE64_DECODE_BINARY($1:vendor_info:mac_prefix::string)) as mac_prefix,
    nvl($1:vendor_info:vendor_name::string,'') as vendor_name,
    nvl($1:vendor_info:non_human, false)::boolean as non_human
    
FROM @ZENPROD.PRESENCE.ARCHIVER_WIFI_ENRICHED_BLIPS_S3_STAGE)
on_error = 'continue';
