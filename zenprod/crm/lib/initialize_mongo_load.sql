alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

use warehouse &{whname};
use database &{dbname};
use role &{rolename};
use schema &{stageschema};

create file format if not exists &{dbname}.&{stageschema}.s3_mongo_json_format
  type = 'JSON'
  strip_outer_array = true;
  
create stage if not exists &{stagename} 
  file_format = s3_mongo_json_format
  url = '&{stageurl}'
  credentials = (aws_key_id='AKIAJY23B2S6FEDPFOKA' aws_secret_key='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');

create function if not exists &{dbname}.&{stageschema}.string_to_mac(A string)
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
