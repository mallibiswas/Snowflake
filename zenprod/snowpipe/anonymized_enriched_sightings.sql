-------------------------------------------------------------------
----------------- Anonymized Enriched Sightings Snowpipe
-------------------------------------------------------------------

create OR replace stage ZENPROD.PRESENCE.ANONYMIZED_ENRICHED_SIGHTINGS_S3_STAGE
    file_format = ( format_name = 'ZENPROD.PRESENCE.S3_PARQUET_FORMAT' )
    url = 's3://zp-uw2-foundation-kafka-archives/secor/anonymizedenrichedsightings_anonymizedenrichedsighting_byanonymizedmac_0/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-anonymizedenrichedsightings-snowflake-stage');

create or replace table ZENPROD.PRESENCE.ANONYMIZED_ENRICHED_SIGHTINGS (
    CLIENT_MAC_ANONYMIZATION VARCHAR(16777216),
    CONTACT_ID VARCHAR(16777216),
    CONTACT_INFO VARCHAR(16777216),
    CONTACT_METHOD VARCHAR(16777216),
    IN_BUSINESS_NETWORK BOOLEAN,
    LOCATION_ID VARCHAR(16777216),
    START_TIME NUMBER(38,0),
    END_TIME NUMBER(38,0),
    IS_WALK_IN BOOLEAN,
    IS_FIRST_CONTACT BOOLEAN,
    BLIP_COUNT NUMBER(38,0),
    MAX_RSSI INTEGER,
    MIN_RSSI INTEGER,
    AVG_RSSI FLOAT,
    STATUS VARCHAR(16777216))
    CLUSTER BY (CONTACT_ID, LOCATION_ID);

create or replace pipe ZENPROD.PRESENCE.ANONYMIZED_ENRICHED_SIGHTINGS_SNOWPIPE auto_ingest=true as
COPY INTO ZENPROD.PRESENCE.ANONYMIZED_ENRICHED_SIGHTINGS
FROM (SELECT lower($1:anonymized_client_mac::string) as client_mac_anonymization,
    lower($1:contact:id::string) as contact_id,
    $1:contact:info::string as contact_info,
    $1:contact:method::string as contact_method,
    $1:in_business_network::boolean as in_business_network,
    lower($1:location_id::string) as business_id,
    $1:start_time::integer as start_time,
    $1:end_time::integer as end_time,
    $1:is_walk_in:value::boolean as is_walk_in,
    $1:is_first_contact:value::boolean as is_first_contact,
    $1:stats:blip_count::integer as blip_count,
    $1:stats:max_rssi::integer as max_rssi,
    $1:stats:min_rssi::integer as min_rssi,
    $1:stats:avg_rssi::float as avg_rssi,
    nvl($1:status,'NEW')::string as status
FROM @ZENPROD.PRESENCE.ANONYMIZED_ENRICHED_SIGHTINGS_S3_STAGE)
on_error = 'continue';
