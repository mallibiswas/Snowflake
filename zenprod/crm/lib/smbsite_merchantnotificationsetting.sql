-----------------------------------------------------------------------
---------------------  smbsite_merchantnotificationsetting ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_merchantnotificationsetting;

copy into &{dbname}.&{stageschema}.smbsite_merchantnotificationsetting
from 
(
select 
  	$1:id::string as merchantnotificationsetting_id,
  	$1:userprofile_id::string as userprofile_id,
  	$1:business_id::string as business_id,
  	$1:date_added::timestamp as date_added,
  	$1:email::string as email,
  	$1:reputation_notification::variant as reputation_notification,
  	$1:updated::timestamp as updated,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_merchantnotificationsetting00000.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_merchantnotificationsetting swap with &{dbname}.&{schemaname}.smbsite_merchantnotificationsetting;


