-----------------------------------------------------------------------
---------------------  smbsite_gatekeeper ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_gatekeeper;

copy into &{dbname}.&{stageschema}.smbsite_gatekeeper
from 
(
select $1:_id:"$oid"::string as gatekeeper_id,
      $1:description::string as description,                  
      $1:name::string as name,                  
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_gatekeeper.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_gatekeeper swap with &{dbname}.&{schemaname}.smbsite_gatekeeper;


