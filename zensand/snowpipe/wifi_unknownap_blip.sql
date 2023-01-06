-------------------------------------------------------------------
----------------- Wifi Unkown AP Blip Snowpipe
-------------------------------------------------------------------

create OR replace stage ZENSAND.PRESENCE.WIFI_UNKNOWNAP_BLIP_S3_STAGE
    file_format = ( format_name = 'ZENSAND.PRESENCE.S3_PARQUET_FORMAT' )
    url = 's3://zd-uw2-platform-kafka-archives/secor/presence_wifi_unknownap_blip_byclientmac/'
    credentials = ( aws_role = 'arn:aws:iam::504740723475:role/zd-uw2-wifi-unknownap-blip-snowflake-stage');

create or replace table ZENSAND.PRESENCE.WIFI_UNKNOWNAP_BLIP (
    AP_TYPE VARCHAR(16777216),
    AP_MAC VARCHAR(16777216),

    CLIENT_MAC VARCHAR(16777216),

    RSSI INTEGER,

    SERVER_TS TIMESTAMP,

    -- There is other fields in the proto, but at this point they will never be populated

    CLIENT_MAC_ANONYMIZATION VARCHAR(16777216)
);

create or replace pipe ZENSAND.PRESENCE.WIFI_UNKNOWNAP_BLIP_SNOWPIPE auto_ingest=true as
COPY INTO ZENSAND.PRESENCE.WIFI_UNKNOWNAP_BLIP
FROM (SELECT 
    nvl($1:ap_type, 'APType_UNKOWN')::string as ap_type,
    ZENSAND.PRESENCE.string_to_mac(lower($1:ap_mac::string)) as ap_mac,

    ZENSAND.PRESENCE.string_to_mac(lower($1:client_mac::string)) as client_mac,

    nvl($1:stats:rssi, 0)::integer as rssi,

    to_timestamp_ntz($1:server_ts:seconds::integer * 1000 + nvl($1:server_ts:nanos, 0)::integer / 1000000, 3) as server_ts,

    $1:client_mac_anonymization::string as client_mac_anonymization

FROM @ZENSAND.PRESENCE.WIFI_UNKNOWNAP_BLIP_S3_STAGE)
on_error = 'continue';
