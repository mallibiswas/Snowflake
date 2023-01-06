alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

-- alter warehouse &{whname} set warehouse_size = MEDIUM;

use warehouse &{whname};
use database &{dbname};
use role &{rolename};

create or replace file format &{dbname}.PUBLIC.ads_csv_format
FIELD_DELIMITER=','
ESCAPE_UNENCLOSED_FIELD='\\'  
ESCAPE = '\\'
RECORD_DELIMITER='\n'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
EMPTY_FIELD_AS_NULL=true
EMPTY_FIELD_AS_NULL=true
null_if=('NULL') 
skip_header = 1;
    
create stage if not exists &{stagename}
 file_format = ads_csv_format
 url = '&{stageurl}'
 credentials = (aws_key_id='AKIAJY23B2S6FEDPFOKA' aws_secret_key='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');

create function if not exists &{dbname}.public.string_to_mac(A string)
  returns string
  language javascript
as
$$
if( A ) {
  return A.match(/.{1,2}/g).join( ':' ).toLowerCase();
}
return '00:00:00:00:00:00'
$$
;

