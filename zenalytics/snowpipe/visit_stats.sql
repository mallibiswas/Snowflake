------------------------------------------------------------
------------------ VISIT STATS SNOWFPIPE -------------------
------------------------------------------------------------

create file format if not exists  s3_parquet_format
  type = 'PARQUET'
  snappy_compression = true
  BINARY_AS_TEXT = false
  strip_outer_array = true;

create or replace stage zenalytics._staging.s3_visit_stats_stage
  file_format = s3_parquet_format
  url = 's3://zp-uw2-foundation-kafka-archives/secor/visitstats_visitstats_byvisitkey_0/'  
credentials = (aws_key_id='*****************' aws_secret_key='*****************');

create function if not exists string_to_mac(A string)
  returns string
  language javascript
as
$$
  return A.match(/.{1,2}/g).join( ':' );
$$
;

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

create or replace pipe ZENALYTICS._STAGING.visit_stats_snowpipe auto_ingest=true as
COPY INTO ZENALYTICS.PRESENCE.visit_stats
FROM
(
SELECT  lower($1:contact_id::string) as contact_id,
        $1:contact_info::string as contact_info,
        $1:contact_method::string as contact_method,
        $1:first_seen_time::integer as first_seen_time,
        $1:last_seen_time::integer as last_seen_time,
        lower($1:location_id)::string as business_id,
        parse_json($1:source) as source,
        $1:status::string as status,
        $1:visit_count::integer as visit_count
FROM @s3_visit_stats_stage
)
on_error = 'continue'
;

show pipes;

show stages

