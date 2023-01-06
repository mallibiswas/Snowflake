-----------------------------------------------------------------------
--------------------- repmanagement_businessrating      --------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.repmanagement_businessrating;

copy into &{dbname}.&{stageschema}.repmanagement_businessrating
from
(
select  $1:_id:"$oid"::string as repmanagement_businessrating_id,
      $1:user_id:"$oid"::string as user_id,
      $1:business_id:"$oid"::string as business_id,
      $1:rating::float as rating,
      $1:created:"$date"::datetime as created,
      $1:updated:"$date"::datetime as updated,
      $1:rating_updated:"$date"::datetime as rating_updated,
      '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/repmanagement_businessrating.json 
);

alter table &{dbname}.&{stageschema}.repmanagement_businessrating swap with &{dbname}.&{schemaname}.repmanagement_businessrating;

