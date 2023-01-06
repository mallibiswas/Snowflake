-----------------------------------------------------------------------
---------------------  smbsite_offerlog ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_offerlog;

copy into &{dbname}.&{stageschema}.smbsite_offerlog
from 
(
select 
    $1:id::string as offerlog_id,
    $1:business_id::string as business_id,
    $1:userprofile_id::string as userprofile_id,
    $1:code::string as offer_code,
    $1:created::timestamp as created,
    $1:expiration::timestamp as expiration,
    $1:offer_id::string as offer_id,
    $1:opened::timestamp as opened,
    $1:redeemed::timestamp as redeemed,
    $1:redirect_id::string as redirect_id,
    $1:test_user::boolean as test_user,
    '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/smbsite_offerlog00000.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_offerlog swap with &{dbname}.&{schemaname}.smbsite_offerlog;


