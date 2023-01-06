-----------------------------------------------------------------------
---------------------  SMBSITE_BLACKLISTEDEMAIL -----------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.SMBSITE_BLACKLISTEDEMAIL;

copy into &{dbname}.&{stageschema}.SMBSITE_BLACKLISTEDEMAIL
from 
(
select  $1:_id:"$oid"::string as blacklistedemail_id,
      $1:email::string as email,
      $1:occurrences::integer as occurrences,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_blacklistedemail.json  
); 

alter table &{dbname}.&{stageschema}.SMBSITE_BLACKLISTEDEMAIL swap with &{dbname}.&{schemaname}.SMBSITE_BLACKLISTEDEMAIL;


