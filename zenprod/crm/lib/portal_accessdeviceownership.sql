-----------------------------------------------------------------------
---------------------  PORTAL ACCESSDEVICEOWNERSHIP -------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.PORTAL_ACCESSDEVICEOWNERSHIP;

copy into &{dbname}.&{stageschema}.PORTAL_ACCESSDEVICEOWNERSHIP
from
(
    select  $1:_id:"$oid"::string AS accessdeviceownership_id,
            $1:accessdevice_id:"$oid"::string AS accessdevice_id,
            $1:created:"$date"::datetime AS created,
            $1:last_confirmed:"$date"::datetime AS last_confirmed,
            $1:userprofile_id:"$oid"::string AS userprofile_id,
		'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/portal_accessdeviceownership.json 
);

alter table &{dbname}.&{stageschema}.PORTAL_ACCESSDEVICEOWNERSHIP swap with &{dbname}.&{schemaname}.PORTAL_ACCESSDEVICEOWNERSHIP;
