-----------------------------------------------------------------------
---------------------   smbsite_importlog ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_importlog;

copy into &{dbname}.&{stageschema}.smbsite_importlog
from 
(
select 
    $1:id::string as importlog_id,
    $1:business_id::string as business_id,
    $1:cioaccount_id::string as cioaccount_id,
    $1:completed::timestamp as completed,
    $1:contacts_added::integer as contacts_added,
    $1:contacts_found::integer as contacts_found,
    $1:emailimport_id::string as emailimport_id,
    $1:imported_by_id::string as imported_by_id,
    $1:source::string as "source",
    $1:started::timestamp as started,
    $1:success::boolean as success,
    $1:tags::variant as tags,
    $1:undone::timestamp as undone,
    $1:username::string as username,
    $1:webhook::boolean as webhook,
    '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/smbsite_importlog00000.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_importlog swap with &{dbname}.&{schemaname}.smbsite_importlog;


