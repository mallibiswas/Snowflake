-----------------------------------------------------------------------
---------------------  SMBSITE_ACTIVITYLOG ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.SMBSITE_ACTIVITYLOG;

copy into &{dbname}.&{stageschema}.SMBSITE_ACTIVITYLOG
from 
(
select  $1:_id:"$oid"::string as activitylog_id,
      $1:business_id:"$oid"::string as business_id,
      $1:user_id:"$oid"::string as user_id,
      $1:activity::string as activity,
      $1:is_staff::boolean as is_staff,
      $1:created:"$date"::datetime as created,
      $1:latest:"$date"::datetime as latest,      
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_activitylog.json  
); 

alter table &{dbname}.&{stageschema}.SMBSITE_ACTIVITYLOG swap with &{dbname}.&{schemaname}.SMBSITE_ACTIVITYLOG;


