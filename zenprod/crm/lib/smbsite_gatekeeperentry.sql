-----------------------------------------------------------------------
---------------------  smbsite_gatekeeperentry ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_gatekeeperentry;

copy into &{dbname}.&{stageschema}.smbsite_gatekeeperentry
from 
(
select  $1:_id:"$oid"::string as gatekeeperentry_id,
      $1:business_id:"$oid"::string as business_id,                  
      $1:gatekeeper_id:"$oid"::string as gatekeepr_id,                  
      $1:created:"$date"::datetime as created,
      $1:removed:"$date"::datetime as removed,                  
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_gatekeeperentry.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_gatekeeperentry swap with &{dbname}.&{schemaname}.smbsite_gatekeeperentry;


