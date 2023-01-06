
use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Product
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.product as 
select 
$1 as product_id,
$2 as product_name,
$3 as product_code,
$4::timestamp as created,
$5::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/product.csv;


alter table &{stageschemaname}.product swap with &{schemaname}.product;
