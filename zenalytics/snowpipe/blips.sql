------------------ PORTAL BLIPS SNOWFPIPE -------------------
create file format if not exists  s3_parquet_format
  type = 'PARQUET'
  snappy_compression = true
  BINARY_AS_TEXT = false
  strip_outer_array = true;

create or replace stage zenalytics._staging.s3_portal_blips_stage
  file_format = s3_parquet_format
  url = 's3://zp-uw2-foundation-kafka-archives/secor/portalblips_blip_byclientmac_0/'  
credentials = (aws_key_id='******************' aws_secret_key='******************');

create function if not exists string_to_mac(A string)
  returns string
  language javascript
as
$$
  return A.match(/.{1,2}/g).join( ':' );
$$
;

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

create or replace pipe ZENALYTICS._STAGING.portal_blips_snowpipe auto_ingest=true as
COPY INTO ZENALYTICS.PRESENCE.portal_blips
FROM
(
SELECT  string_to_mac(lower($1:client_mac::string)) as client_mac,
        string_to_mac(lower($1:sensor_mac::string)) as sensor_mac,
        $1:sensor_type::string as sensor_type,
        $1:server_time:seconds::integer as server_time,
        $1:ts:seconds::integer as ts,
        $1:value::string as value
FROM @s3_portal_blips_stage
)
on_error = 'continue'
;

------------------ LOCATION BLIPS SNOWFPIPE -------------------
create file format if not exists  s3_parquet_format
  type = 'PARQUET'
  snappy_compression = true
  BINARY_AS_TEXT = false
  strip_outer_array = true;

create or replace stage zenalytics._staging.s3_location_blips_stage
  file_format = s3_parquet_format
  url = 's3://zp-uw2-foundation-kafka-archives/secor/location_blip_0/'  
credentials = (aws_key_id='*************' aws_secret_key='***************');

create function if not exists string_to_mac(A string)
  returns string
  language javascript
as
$$
  return A.match(/.{1,2}/g).join( ':' );
$$
;

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

create or replace pipe ZENALYTICS._STAGING.location_blips_snowpipe auto_ingest=true as
COPY INTO ZENALYTICS.PRESENCE.location_blips
FROM
(
SELECT  string_to_mac(lower($1:client_mac::string)) as client_mac,
        string_to_mac(lower($1:sensor_mac::string)) as sensor_mac,
        $1:sensor_type::string as sensor_type,
        $1:server_time:seconds::integer as server_time,
        $1:ts:seconds::integer as ts,
        $1:value::string as value
FROM @s3_location_blips_stage
)
on_error = 'continue'
;

show pipes;

show stages


