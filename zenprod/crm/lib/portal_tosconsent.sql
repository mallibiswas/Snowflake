-----------------------------------------------------------------------
--------------------- portal_tosconsent      --------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.portal_tosconsent;

copy into &{dbname}.&{stageschema}.portal_tosconsent
from
(
select  $1:_id:"$oid"::string as portal_tosconsent_id,
      $1:business_id:"$oid"::string as business_id,
      $1:userprofile_id:"$oid"::string as userprofile_id,
      string_to_mac($1:client_mac::string) as client_mac,
      $1:created:"$date"::datetime as created,
      '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/portal_tosconsent.json 
);

alter table &{dbname}.&{stageschema}.portal_tosconsent swap with &{dbname}.&{schemaname}.portal_tosconsent;

