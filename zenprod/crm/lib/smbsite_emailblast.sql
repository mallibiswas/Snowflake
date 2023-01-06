-----------------------------------------------------------------------
---------------------  smbsite_emailblast ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_emailblast;

copy into &{dbname}.&{stageschema}.smbsite_emailblast
from 
(
select $1:_id:"$oid"::string as emailblast_id,
      $1:business_id:"$oid"::string as business_id,
      $1:campaign_id:"$oid"::string as campaign_id,
      $1:bounced_filtered_count::integer as bounced_filtered_count,
      $1:created:"$date"::datetime as created,
      $1:invalid_filtered_count::integer as invalid_filtered_count,
      $1:message_id:"$oid"::string as message_id,
      $1:queued:"$date"::datetime as queued,
      $1:queued_count::integer as queued_count,
      $1:target_id:"$oid"::string as target_id,
      $1:target_size::string as target_size,
      $1:scheduled:"$date"::datetime as scheduled,
      $1:sending:"$date"::datetime as sending,
      $1:sent:"$date"::datetime as sent,
      $1:unsubscribed_filtered_count::integer as unsubscribed_filtered_count,
      $1:unvisited_customers::integer as unvisited_customers,
      $1:employee_filtered_count::integer as employee_filtered_count,
      $1:invalid_target_filtered_count::integer as invalid_target_filtered_count,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_emailblast.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_emailblast swap with &{dbname}.&{schemaname}.smbsite_emailblast;


