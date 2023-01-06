-----------------------------------------------------------------------
---------------------  SMBSITE_DEFAULTTRIGGER ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.SMBSITE_DEFAULTTRIGGER;

copy into &{dbname}.&{stageschema}.SMBSITE_DEFAULTTRIGGER
from 
(
select $1:_id:"$oid"::string as defaulttrigger_id,
      $1:business_id:"$oid"::string as business_id,      
      $1:message_id:"$oid"::string as message_id,
      $1:demographic_rule::string as demographic_rule,
      $1:description::string as description,
      $1:enabled::boolean as enabled,
      $1:is_recurring::boolean as is_recurring,
      $1:linked::boolean as linked,
      $1:parent_id::string as parent_id,
      replace($1:parameters::variant,'\\','') as parameters,
      replace($1:proximity_rule::variant,'\\','') as proximity_rule,
      replace($1:purchase_rule::variant,'\\','') as purchase_rule,
      replace($1:rule::variant,'\\','') as rule,
      $1:timebox::integer as timebox,
      $1:title::string as title,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_defaulttrigger.json  
); 

alter table &{dbname}.&{stageschema}.SMBSITE_DEFAULTTRIGGER swap with &{dbname}.&{schemaname}.SMBSITE_DEFAULTTRIGGER;


