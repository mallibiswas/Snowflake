-----------------------------------------------------------------------
---------------------  smbsite_offer ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_offer;

copy into &{dbname}.&{stageschema}.smbsite_offer
from 
(
select 
    $1:id::string as offer_id,
    $1:business_id::string as business_id,
    $1:email_blast_id::string as email_blast_id,
    $1:expiration::timestamp as expiration,
    $1:future_expiration::timestamp as future_expiration,
    $1:logo_id::string as logo_id,
    $1:require_mac::string as require_mac,
    $1:test::string as free_hug,
    $1:title::string as title,
    $1:trigger_id::string as trigger_id,
     '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/smbsite_offer00000.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_offer swap with &{dbname}.&{schemaname}.smbsite_offer;


