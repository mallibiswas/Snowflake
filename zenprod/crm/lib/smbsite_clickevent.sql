-----------------------------------------------------------------------
---------------------  SMBSITE__CLICKEVENT ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.SMBSITE_CLICKEVENT;

copy into &{dbname}.&{stageschema}.SMBSITE_CLICKEVENT
from 
(
select  $1:_id:"$oid"::string as blacklistedemail_id,
      $1:blast_id::string as blast_id,
      $1:business_id:"$oid"::string as business_id,
      $1:userprofile_id:"$oid"::string as userprofile_id,
      $1:long_url::string as long_url,
      $1:short_url::string as short_url,
      $1:click_time:"$date"::datetime as click_time,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_clickevent.json  
); 

alter table &{dbname}.&{stageschema}.SMBSITE_CLICKEVENT swap with &{dbname}.&{schemaname}.SMBSITE_CLICKEVENT;


