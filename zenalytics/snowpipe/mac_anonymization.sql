------------------------------------------------------------------
------------------ MAC ANONYMIZATION SNOWFPIPE -------------------
------------------------------------------------------------------

create file format if not exists s3_parquet_format
  type = 'PARQUET'
  snappy_compression = true
  BINARY_AS_TEXT = false
  strip_outer_array = true;

create or replace stage s3_anonymizer_stage
  file_format = s3_parquet_format
  url = 's3://zp-uw2-foundation-kafka-archives/secor/macanonymizations_macanonymization_byclientmac_0/'
  credentials = (aws_key_id='*************' aws_secret_key='******************');

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
create or replace pipe ZENALYTICS._STAGING.anonymizer_snowpipe auto_ingest=true as
COPY INTO ZENALYTICS.PRESENCE.CLIENT_MAC_ANONYMIZED
FROM
(
select $1:anonymization::string as client_mac_anonymized,
string_to_mac(lower($1:mac))::string as client_mac,
$1:ts:seconds::integer as ts
from @s3_anonymizer_stage
)
on_error = 'continue'
;

show pipes;

show stages


