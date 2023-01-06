-----------------------------------------------------------------------
---------------------  smbsite_mergedblast ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_mergedblast;

copy into &{dbname}.&{stageschema}.smbsite_mergedblast
from 
(
select 
      $1:id::string as mergedblast_id,
      $1:business_id::string as business_id,
      $1:created::timestamp as created,
      $1:deleted::timestamp as deleted,
      $1:draft::boolean as draft,
      $1:email_blast_id::string as email_blast_id,
      $1:scheduled::timestamp as scheduled,
      $1:sms_blast_id::string as sms_blast_id,
      $1:sort_key::integer as sort_key,
      $1:target::variant as target,
      '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/smbsite_mergedblast00000.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_mergedblast swap with &{dbname}.&{schemaname}.smbsite_mergedblast;


