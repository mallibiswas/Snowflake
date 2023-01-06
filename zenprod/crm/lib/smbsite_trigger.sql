-----------------------------------------------------------------------
---------------------  smbsite_trigger ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_trigger;

copy into &{dbname}.&{stageschema}.smbsite_trigger
from 
(
select 
    $1:id::string as trigger_id,
    $1:business_id::string as business_id,
    $1:demographic_rule::string as demographic_rule,
    $1:enabled::boolean as enabled,
    $1:event_driven::boolean as event_driven,
    $1:free::boolean as free,
    $1:hidden::boolean as hidden,
    $1:is_custom::boolean as is_custom,
    $1:is_recurring::boolean as is_recurring,
    $1:linked::boolean as linked,
    $1:message_id::string as message_id,
    parse_json($1:parameters)::variant as parameters,
    $1:parent_id::string as parent_id,
    $1:purchase_rule::string as purchase_rule,
    parse_json($1:rule)::variant as rule,
    $1:timebox::float as timebox,
    $1:title::string as title,
    '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/smbsite_trigger00000.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_trigger swap with &{dbname}.&{schemaname}.smbsite_trigger;


