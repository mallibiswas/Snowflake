-----------------------------------------------------------------------
---------------------  privacy_response -------------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.PRIVACY_RESPONSE;

copy into &{dbname}.&{stageschema}.PRIVACY_RESPONSE
from 
(
select 
  $1:id::string as response_id,
  $1:request_id::string as request_id,
  parse_json($1:payload::variant) as payload,
  $1:source::string as source,
  $1:created::timestamp_tz as created,
  '&{asof_date}'::date as asof_date
  from @&{stagename}/&{stagepath}/&{stagefile}
); 

alter table &{dbname}.&{stageschema}.PRIVACY_RESPONSE swap with &{dbname}.&{schemaname}.PRIVACY_RESPONSE;


