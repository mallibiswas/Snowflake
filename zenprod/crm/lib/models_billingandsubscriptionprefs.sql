-----------------------------------------------------------------------
---------------------  models_billingandsubscriptionprefs ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.models_billingandsubscriptionprefs;

copy into &{dbname}.&{stageschema}.models_billingandsubscriptionprefs
from 
(
select  $1:_id:"$oid"::string as billingandsubscriptionprefs_id,
      $1:business_id:"$oid"::string as business_id,                  
      $1:created:"$date"::datetime as created,
      $1:enrichment_enabled::boolean as enrichment_enabled,      
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/models_billingandsubscriptionprefs.json  
); 

alter table &{dbname}.&{stageschema}.models_billingandsubscriptionprefs swap with &{dbname}.&{schemaname}.models_billingandsubscriptionprefs;


