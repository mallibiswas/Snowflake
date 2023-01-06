-----------------------------------------------------------------------
---------------------  mobile_engagementlog ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.mobile_engagementlog;

copy into &{dbname}.&{stageschema}.mobile_engagementlog
from 
(
select  $1:_id:"$oid"::string as engagementlog_id,
      $1:sent:"$date"::datetime as sent,
      $1:device_id::string as device_id,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/mobile_engagementlog.json  
); 

alter table &{dbname}.&{stageschema}.mobile_engagementlog swap with &{dbname}.&{schemaname}.mobile_engagementlog;


