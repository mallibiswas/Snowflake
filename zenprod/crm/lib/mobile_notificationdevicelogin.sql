-----------------------------------------------------------------------
---------------------  mobile_notificationdevicelogin ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.mobile_notificationdevicelogin;

copy into &{dbname}.&{stageschema}.mobile_notificationdevicelogin
from 
(
select  $1:_id:"$oid"::string as notificationdevicelogin_id,
      $1:device_id::string as device_id,
      $1:archived:"$date"::datetime as archived,
      $1:last_login:"$date"::datetime as last_login,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/mobile_notificationdevicelogin.json  
); 

alter table &{dbname}.&{stageschema}.mobile_notificationdevicelogin swap with &{dbname}.&{schemaname}.mobile_notificationdevicelogin;


