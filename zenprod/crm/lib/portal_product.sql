-----------------------------------------------------------------------
--------------------- portal_product      -----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.portal_product;

copy into &{dbname}.&{stageschema}.portal_product
from
(
select  $1:_id:"$oid"::string as product_id,
      $1:date_added:"$date"::datetime as date_added,
      $1:name::string as name,
      '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/portal_product.json 
);

alter table &{dbname}.&{stageschema}.portal_product swap with &{dbname}.&{schemaname}.portal_product;

