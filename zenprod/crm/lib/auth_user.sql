-----------------------------------------------------------------------
--------------------- auth_user      --------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.auth_user;

copy into &{dbname}.&{stageschema}.auth_user
from
(
select  $1:_id:"$oid"::string as user_id,
      $1:email::string as email,
      $1:date_joined:"$date"::datetime as date_joined,
      $1:username::string as username,
      $1:is_active::boolean as is_active,
      $1:last_login:"$date"::datetime as last_login,
      '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/auth_user.json 
);

alter table &{dbname}.&{stageschema}.auth_user swap with &{dbname}.&{schemaname}.auth_user;

