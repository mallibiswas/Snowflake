-----------------------------------------------------------------------
---------------------  smbsite_emailtemplate ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_emailtemplate;

copy into &{dbname}.&{stageschema}.smbsite_emailtemplate
from 
(
select  $1:_id:"$oid"::string as emailtemplate_id,
      $1:business_id:"$oid"::string as business_id,                  
      $1:created:"$date"::datetime as created,
      $1:complete::boolean as complete,
      parse_json($1:layout)::variant as layout,
      $1:subject::string as subject,
      $1:updated:"$date"::datetime as updated,      
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_emailtemplate.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_emailtemplate swap with &{dbname}.&{schemaname}.smbsite_emailtemplate;


