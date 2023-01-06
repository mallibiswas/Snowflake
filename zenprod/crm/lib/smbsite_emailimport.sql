-----------------------------------------------------------------------
---------------------  smbsite_emailimport ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_emailimport;

copy into &{dbname}.&{stageschema}.smbsite_emailimport
from 
(
select  $1:_id:"$oid"::string as emailimport_id,
      $1:business_id:"$oid"::string as business_id,                  
      $1:created:"$date"::datetime as created,
      $1:error::string as error,
      $1:file as file,
      $1:uploaded_by_id:"$oid"::string as uploaded_by_id,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_emailimport.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_emailimport swap with &{dbname}.&{schemaname}.smbsite_emailimport;


