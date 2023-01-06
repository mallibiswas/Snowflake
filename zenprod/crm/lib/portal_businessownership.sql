---------------------------------------------------------------------------------
--------------------- portal_businessownership --------------------------
---------------------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.portal_businessownership;

copy into &{dbname}.&{stageschema}.portal_businessownership
from
(
select  $1:_id:"$oid"::string as businessownership_id,
      $1:business_id:"$oid"::string as business_id,
      $1:userprofile_id:"$oid"::string as userprofile_id,
      $1:create:"$date"::datetime as created,
      $1:updated:"$date"::datetime as updated,
      $1:role_ids::variant as role_ids,
      '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/portal_businessownership.json 
);

alter table &{dbname}.&{stageschema}.portal_businessownership swap with &{dbname}.&{schemaname}.portal_businessownership;

