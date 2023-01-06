-----------------------------------------------------------------------
---------------------  PORTAL ACCESSDEVICE ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.PORTAL_ACCESSDEVICE;

copy into  &{dbname}.&{stageschema}.PORTAL_ACCESSDEVICE
from
(
    select  $1:_id:"$oid"::string AS accessdevice_id,
            $1:cookie_key::string AS cookie_key, 
            $1:date_added:"$date"::datetime AS date_added,
            $1:last_seen:"$date"::datetime AS last_seen,
            $1:last_seen_ip::string AS last_seen_ip,
            lower($1:mac::string) AS mac,
		'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/portal_accessdevice.json 
)
on_error='CONTINUE'
;

alter table &{dbname}.&{stageschema}.PORTAL_ACCESSDEVICE swap with &{dbname}.&{schemaname}.PORTAL_ACCESSDEVICE;
