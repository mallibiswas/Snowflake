-----------------------------------------------------------------------
---------------------  mobile_deviceregistrationlog ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.mobile_deviceregistrationlog;

copy into &{dbname}.&{stageschema}.mobile_deviceregistrationlog
from 
(
select  $1:_id:"$oid"::string as deviceregistrationlog_id,
      $1:created:"$date"::datetime as created,
      $1:device_id::string as device_id,
      $1:login_email::string as login_email,
      $1:push_id::string as push_id,      
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/mobile_deviceregistrationlog.json  
); 

alter table &{dbname}.&{stageschema}.mobile_deviceregistrationlog swap with &{dbname}.&{schemaname}.mobile_deviceregistrationlog;


