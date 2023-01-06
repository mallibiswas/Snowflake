-----------------------------------------------------------------------
---------------------  smbsite_target ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_target;

copy into &{dbname}.&{stageschema}.smbsite_target
from 
(
select 
    $1:business_id::string as business_id,
    $1:created::timestamp as created,
    $1:id::string as target_id,
    $1:post_data::variant as post_data,
    '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/smbsite_target00000.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_target swap with &{dbname}.&{schemaname}.smbsite_target;


