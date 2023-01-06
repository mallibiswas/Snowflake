-----------------------------------------------------------------------
---------------------  mobile_notificationdevice ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.mobile_notificationdevice;

copy into &{dbname}.&{stageschema}.mobile_notificationdevice
from 
(
select  $1:_id:"$oid"::string as notificationdevice_id,
      $1:business_id:"$oid"::string as business_id,                  
      $1:device_id::string as device_id,
      $1:created:"$date"::datetime as created,
      $1:archived:"$date"::datetime as archived,
      $1:last_updated:"$date"::datetime as last_updated,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/mobile_notificationdevice.json  
); 

alter table &{dbname}.&{stageschema}.mobile_notificationdevice swap with &{dbname}.&{schemaname}.mobile_notificationdevice;


