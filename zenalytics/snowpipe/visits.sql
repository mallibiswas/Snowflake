----------------------------------------
------ CREATE SNOWPIPE FOR VISITS ------
----------------------------------------

use role sysadmin;
use database zenalytics;
use schema _STAGING;

create or replace file format s3_parquet_format
  type = 'PARQUET'
  snappy_compression = true
  BINARY_AS_TEXT = false
  strip_outer_array = true;

create or replace stage s3_visits_stage
  file_format = s3_parquet_format
  url = 's3://zp-uw2-foundation-kafka-archives/secor/visits_visit_byvisitkey_0/'
  credentials = (aws_key_id='******' aws_secret_key='**************');

create function if not exists string_to_mac(A string)
  returns string
  language javascript
as
$$
  return A.match(/.{1,2}/g).join( ':' );
$$
;

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

-- Create a pipe to ingest JSON data
create or replace pipe ZENALYTICS._STAGING.visits_snowpipe auto_ingest=true as
COPY INTO ZENALYTICS.PRESENCE.VISITS
FROM
(
SELECT  string_to_mac(lower($1:client_mac::string)) as client_mac,
      lower($1:contact_id::string) AS contact_id,
      $1:contact_info::string AS contact_info,
      $1:contact_method::string AS contact_method,
      $1:end_time::integer AS end_time,
      $1:in_business_network::boolean AS in_business_network,
      lower($1:location_id::string) AS business_id,
      $1:server_end_time::integer AS server_end_time,
      $1:source:id::string AS id,
      $1:source:name::string AS name,
      $1:start_time::integer AS start_time,
      $1:status::string AS status
FROM @s3_visits_stage
)
;



