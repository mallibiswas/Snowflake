-----------------------------------------------------------------------
---------------------  SMBSITE__CLICKLOG   ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.SMBSITE_CLICKLOG;

copy into &{dbname}.&{stageschema}.SMBSITE_CLICKLOG
from 
(
select  $1:_id:"$oid"::string as clicklog_id,
      $1:message_id:"$oid"::string as message_id,
      $1:messagelog_id:"$oid"::string as messagelog_id,
      $1:timestamp:"$date"::datetime as timestamp,
      $1:client_os::string as client_os,
      $1:client_type::string as client_type,
      $1:device_type::string as device_type,
      $1:link_class::string as link_class,      
      $1:url::string as url,
      $1:user_agent::string as user_agent,            
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_clicklog.json  
); 

alter table &{dbname}.&{stageschema}.SMBSITE_CLICKLOG swap with &{dbname}.&{schemaname}.SMBSITE_CLICKLOG;


