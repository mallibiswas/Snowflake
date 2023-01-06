--------------------------------------------------------
------ CREATE SNOWPIPE FOR MAIL HOOK -------------------
--------------------------------------------------------

use role sysadmin;
use database zenalytics;
use schema _STAGING;

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

create file format if not exists s3_parquet_format
  type = 'PARQUET'
  snappy_compression = true
  BINARY_AS_TEXT = false
  strip_outer_array = true;

/* Create a temporary internal stage that references the file format object. Temporary stages are automatically dropped at the end of the session. */
  
create or replace stage s3_mail_hook_stage
  file_format = s3_parquet_format
  url = 's3://zp-uw2-platform-kafka-archives/secor/mail_hook_0/'
  credentials = (aws_key_id='**************' aws_secret_key='*******************');


create function if not exists string_to_mac(A string)
  returns string
  language javascript
as
$$
  return A.match(/.{1,2}/g).join( ':' );
$$
;


-- Create a pipe to ingest JSON data
create or replace pipe ZENALYTICS._STAGING.mail_hook_snowpipe auto_ingest=true as
COPY INTO ZENALYTICS._STAGING._MAIL_HOOK_0_ (arguments, source, envelope, type, insert_dttm)
FROM
(
SELECT  parse_json(replace($1:arguments,'z:',''))::variant as arguments, 
        $1:source as source, 
        $1:envelope as envelope, 
        $1:type as type,
        current_timestamp() as insert_dttm
FROM @s3_mail_hook_stage
)
;

