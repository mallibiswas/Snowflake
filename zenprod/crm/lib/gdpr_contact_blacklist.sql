-----------------------------------------------------------------------
--------------------- gdpr_contact_blacklist --------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.gdpr_contact_blacklist;

copy into &{dbname}.&{stageschema}.gdpr_contact_blacklist
from
(
select  $1:_id:"$oid"::string as gdpr_contact_blacklist_id,
      $1:contact_id:"$oid"::string as contact_id,
      $1:account_id:"$oid"::string as account_id,
      '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/gdpr_contact_blacklist.json 
);

alter table &{dbname}.&{stageschema}.gdpr_contact_blacklist swap with &{dbname}.&{schemaname}.gdpr_contact_blacklist;

