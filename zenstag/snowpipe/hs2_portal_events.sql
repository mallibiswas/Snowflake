-------------------------------------------------------------------
-----------------Hs2_Portal_Events Snowpipe Using The New Archiver
-------------------------------------------------------------------

create OR replace stage  ZENSTAG.PRESENCE.ARCHIVER_HS2_PORTALEVENTS_S3_STAGE
    file_format = ( TYPE = JSON )
    url = 's3://zs-uw2-platform-kafka-archives/archiver/hs2_portalevents_byid/'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zs-uw2-archiver-hs2-portalevents-snowflake-stage');

create or replace table ZENSTAG.PRESENCE.ARCHIVER_HS2_PORTALEVENTS (
    ID VARCHAR(16777216),
    TIMESTAMP TIMESTAMP,
    LOCATION_ID VARCHAR(16777216),
    SESSION_ID VARCHAR(16777216),
    EVENT_HANDLER VARCHAR(16777216),
    EVENT_TYPE VARCHAR(16777216),
    EVENT_CONTEXT VARCHAR(16777216),
    USER_AGENT VARCHAR(16777216),
    MESSAGE VARCHAR(16777216),
    DEVICE_TYPE VARCHAR(16777216),
    DEVICE_VERSION VARCHAR(16777216),
    HS2_FLOW VARCHAR(16777216),
    HS2_PORTAL_EXPERIENCE VARCHAR(16777216)
);


create or replace pipe ZENSTAG.PRESENCE.ARCHIVER_HS2_PORTALEVENTS_SNOWPIPE auto_ingest=true as
COPY INTO ZENSTAG.PRESENCE.ARCHIVER_HS2_PORTALEVENTS
FROM (SELECT 
        HEX_ENCODE(BASE64_DECODE_BINARY($1:id::string)) as id,
        $1:timestamp::timestamp as timestamp,
        HEX_ENCODE(BASE64_DECODE_BINARY($1:location_id::string)) as location_id,
        HEX_ENCODE(BASE64_DECODE_BINARY($1:session_id::string)) as session_id,
        $1:event_handler::string as event_handler,
        $1:event_type::string as event_type,
        $1:event_context::string as event_context,
        $1:user_agent::string as user_agent,
        $1:message::string as message,
        $1:device_type::string as device_type,
        $1:device_version::string as device_version,
        $1:hs2_flow::string as hs2_flow,
        $1:hs2_portal_experience::string as hs2_portal_experience
     FROM @ZENSTAG.PRESENCE.ARCHIVER_HS2_PORTALEVENTS_S3_STAGE)
on_error = 'continue';
