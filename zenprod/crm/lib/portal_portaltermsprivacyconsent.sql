---------------------------------------------------------------------------------
--------------------- portal_portaltermsprivacyconsent --------------------------
---------------------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.portal_portaltermsprivacyconsent;

copy into &{dbname}.&{stageschema}.portal_portaltermsprivacyconsent
from
(
select  $1:_id:"$oid"::string as termsprivacyconsent_id,
      $1:business_id:"$oid"::string as business_id,
      $1:userprofile_id:"$oid"::string as userprofile_id,
      $1:terms_privacy_bundle_version::string as terms_privacy_bundle_version,
      $1:created:"$date"::datetime as created,
      $1:consent_time:"$date"::datetime as consent_time,
      $1:userprofile_email::string as userprofile_email,
      string_to_mac($1:client_mac::string) as client_mac,      
      '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/portal_portaltermsprivacyconsent.json 
);

alter table &{dbname}.&{stageschema}.portal_portaltermsprivacyconsent swap with &{dbname}.&{schemaname}.portal_portaltermsprivacyconsent;

