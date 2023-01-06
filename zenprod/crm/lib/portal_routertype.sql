-----------------------------------------------------------------------
---------------------  PORTAL ROUTERTYPE ------------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.PORTAL_ROUTERTYPE;

copy into &{dbname}.&{stageschema}.PORTAL_ROUTERTYPE
from 
(
select  $1:_id::string as routertype_id,
      $1:comment::string as "comment", 
      $1:router_version::string as router_version,
      $1:is_custom_firmware::boolean as is_custom_firmware,
      $1:known_unsupportable::boolean as known_unsupportable,
      $1:auth_technique::string as auth_technique,
      $1:workflow_class::string as workflow_classs,
      $1:router_make::string as router_make,
      $1:custom_firmware_file_1::string as custom_firmware_file_1,
      $1:custom_firmware_file_2::string as custom_firmware_file_2,
      $1:default_username::string as default_username,
      $1:software_version::string as software_version,
      $1:wan_ifname::string as wan_ifname,
      $1:router_model::string as router_model,
      $1:stock_firmware_file::string as stock_firmware_file,
      $1:non_consumer as non_consumer,
--      $1:default_password::string as default_password,
      $1:takeover_technique::string as takeover_technique,
      $1:custom_routertype_id::string as custom_routertype_id,
      $1:configuration_logic_id:"$oid"::string as configuration_logic_id,
      replace($1:analytics_detection_logic::variant,'\\','') as analytics_detection_logic,
      $1:override_custom_logic_id:"$oid"::string as override_custom_logic_id,
      $1:detection_logic::variant as detection_logic,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/portal_routertype.json  
); 

alter table &{dbname}.&{stageschema}.PORTAL_ROUTERTYPE swap with &{dbname}.&{schemaname}.PORTAL_ROUTERTYPE;


