-----------------------------------------------------------------------
---------------------  smbsite_message ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_message;

copy into &{dbname}.&{stageschema}.smbsite_message
from 
(
select 
   	$1:id::string as message_id,
    	$1:archived::boolean as archived,
    	$1:body::string as body,
   	$1:business_id::string as business_id,
    	$1:created::timestamp as created,
    	$1:daily_limit::integer as daily_limit,
    	$1:from_address::string as from_address,
    	$1:from_name::string as from_name,
   	$1:is_deprecated::boolean as is_deprecated,
    	$1:is_referenced::boolean as is_referenced,
    	$1:offer_id::string as offer_id,
    	$1:purpose::string as purpose,
    	$1:subject::string as subject,
    	$1:template_id::string as template_id,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_message00000.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_message swap with &{dbname}.&{schemaname}.smbsite_message;


