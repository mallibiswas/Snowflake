-----------------------------------------------------------------------
--------------------- portal_productversion ---------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.portal_productversion;

copy into &{dbname}.&{stageschema}.portal_productversion
from
(
select  $1:_id:"$oid"::string as productversion_id,
      $1:product_id:"$oid"::string as product_id,
      $1:date_added:"$date"::datetime as date_added,
      $1:name::integer as name,
      $1:label::string as label,
      '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/portal_productversion.json 
);

alter table &{dbname}.&{stageschema}.portal_productversion swap with &{dbname}.&{schemaname}.portal_productversion;

