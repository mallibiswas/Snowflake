-------------------------------------------------------------------
-----------------Hs2_Profile Events Snowpipe Using The New Archiver
-------------------------------------------------------------------

create OR replace stage  ZENPROD.PRESENCE.ARCHIVER_HS2_PROFILEEVENTS_S3_STAGE
    file_format = ( TYPE = JSON )
    url = 's3://zp-uw2-foundation-kafka-archives/archiver/hs2_profileevents_byprofileid/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-archiver-hs2-profileevents-snowflake-stage');

create or replace table ZENPROD.PRESENCE.ARCHIVER_HS2_PROFILEEVENTS (
    EVENT_TYPE VARCHAR(16777216),
    PROFILE_ID VARCHAR(16777216),
    CONTACT_ID VARCHAR(16777216),
    LOCATION_ID VARCHAR(16777216),
    TS TIMESTAMP,
    SESSION_ID VARCHAR(16777216),
    CLIENTMAC_HASH VARCHAR(16777216),
    APMAC VARCHAR(16777216)
);


create or replace pipe ZENPROD.PRESENCE.ARCHIVER_HS2_PROFILEEVENTS_SNOWPIPE auto_ingest=true as
COPY INTO ZENPROD.PRESENCE.ARCHIVER_HS2_PROFILEEVENTS
FROM (SELECT 
       $1:event_type::string as event_type,
       HEX_ENCODE(BASE64_DECODE_BINARY($1:profile_id::string)) as profile_id,
       HEX_ENCODE(BASE64_DECODE_BINARY($1:contact_id::string)) as contact_id,
       HEX_ENCODE(BASE64_DECODE_BINARY($1:location_id::string)) as location_id,
       $1:ts::timestamp as ts,
       $1:session_id::string as session_id,
       HEX_ENCODE(BASE64_DECODE_BINARY($1:clientmac_hash::string)) as clientmac_hash,
       HEX_ENCODE(BASE64_DECODE_BINARY($1:apmac::string)) as apmac
     FROM @ZENPROD.PRESENCE.ARCHIVER_HS2_PROFILEEVENTS_S3_STAGE)
on_error = 'continue';
