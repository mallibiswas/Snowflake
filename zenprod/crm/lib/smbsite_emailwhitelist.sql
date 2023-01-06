-----------------------------------------------------------------------
---------------------  smbsite_emailwhitelist ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_emailwhitelist;

copy into &{dbname}.&{stageschema}.smbsite_emailwhitelist
from 
(
select  $1:_id:"$oid"::string as emailwhitelist_id,
      $1:email::string as email,                  
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_emailwhitelist.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_emailwhitelist swap with &{dbname}.&{schemaname}.smbsite_emailwhitelist;


