-----------------------------------------------------------------------
---------------------  smbsite_modileappdownload ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_mobileappdownload;

copy into &{dbname}.&{stageschema}.smbsite_mobileappdownload
from 
(
select 
    $1:id::string as mobileappdownload_id,
    $1:banner_dismissed::timestamp as banner_dismissed,
    $1:banner_first_seen::timestamp as banner_first_seen,
    $1:engagement_type::string as engagement_type,
    $1:user_id::string as user_id,
    $1:view_type::string as view_type,
    '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/smbsite_modileappdownload00000.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_mobileappdownload swap with &{dbname}.&{schemaname}.smbsite_mobileappdownload;


