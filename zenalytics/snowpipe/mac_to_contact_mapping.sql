--------------------------------------------------------
------ CREATE SNOWPIPE FOR MAC TO CONTACT MAPPING ------
--------------------------------------------------------

-- grant all on file format s3_mongo_parquet_format to snowpipe_role

use role sysadmin;
use database zenalytics;
use schema _STAGING;

create file format if not exists s3_parquet_format
  type = 'PARQUET'
  snappy_compression = true
  BINARY_AS_TEXT = false
  strip_outer_array = true;
 
create or replace stage s3_mactocontactmappings_stage
  file_format = s3_parquet_format
  url = 's3://zp-uw2-foundation-kafka-archives/secor/mactocontactmappings_contactmapping_byclientmac_0/'
  credentials = (aws_key_id='******' aws_secret_key='**************');

-- Create a pipe to ingest JSON data
create or replace pipe ZENALYTICS._STAGING.mactocontactmappings_snowpipe auto_ingest=true as
COPY INTO ZENALYTICS._STAGING._mactocontactmappings_contactmapping_byclientmac_0_
(jsontext, insert_dttm)
FROM
(
SELECT  $1::variant as jsontext,
        current_timestamp() as insert_dttm        
FROM @s3_mactocontactmappings_stage
)
;


