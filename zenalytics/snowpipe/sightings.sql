-------------------------------------------
------ CREATE SNOWPIPE FOR SIGHTINGS ------
------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

create file format if not exists s3_parquet_format
  type = 'PARQUET'
  snappy_compression = true
  BINARY_AS_TEXT = false
  strip_outer_array = true;


/* Create a temporary internal stage that references the file format object. Temporary stages are automatically dropped at the end of the session. */
  
create or replace stage s3_sightings_stage
  file_format = s3_parquet_format
  url = 's3://zp-uw2-foundation-kafka-archives/secor/sightings_sighting_byclientmac_0/'
  credentials = (aws_key_id='************' aws_secret_key='******************');


create function if not exists string_to_mac(A string)
  returns string
  language javascript
as
$$
  return A.match(/.{1,2}/g).join( ':' );
$$
;

CREATE OR REPLACE pipe zenalytics._staging.sightings_snowpipe auto_ingest=true as
COPY INTO ZENALYTICS.PRESENCE.SIGHTINGS (business_id,start_time,end_time,client_mac,is_walk_in,source,status)
FROM
(
SELECT  
lower($1:location_id::string) AS business_id,
$1:start_time::integer AS start_time,
$1:end_time::integer as end_time,
string_to_mac(lower($1:client_mac::string)) as client_mac,
$1:is_walk_in:value::boolean as is_walk_in,
$1:source::variant as source,
$1:status::string AS status
FROM @s3_sightings_stage
)
on_error = 'continue'
;

