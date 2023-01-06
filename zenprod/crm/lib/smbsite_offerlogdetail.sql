-----------------------------------------------------------------------
---------------------  smbsite_offerlogdetail ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_offerlogdetail;

copy into &{dbname}.&{stageschema}.smbsite_offerlogdetail
from 
(
select 
    $1:id::string as offerlogdetail_id,
    $1:event::string as event,
    $1:is_error::boolean as is_error,
    $1:offer_log_id::string as offerlog_id,
    $1:response::string as response,
    $1:sms_blast_id::string as sms_blast_id,
    $1:timestamp::timestamp as timestamp,
    $1:url::string as url,
    '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/smbsite_offerlogdetail00000.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_offerlogdetail swap with &{dbname}.&{schemaname}.smbsite_offerlogdetail;


