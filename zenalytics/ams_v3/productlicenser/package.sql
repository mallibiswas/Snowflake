use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Package
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.package as 
select 
$1 as package_id,
$2 as product_id,
$3 as package_name,
$4 as package_code,
$5::timestamp as created,
$6::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/package.csv;

alter table &{stageschemaname}.package swap with &{schemaname}.package;

--------------------------------------------------------------------
-------------- Package Feature Link
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.package_feature_link as 
select 
$1 as package_feature_link,
$2 as package_id,
$3 as feature_id,
$4::timestamp as created,
$5::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/package_feature_link.csv;

alter table &{stageschemaname}.package_feature_link swap with &{schemaname}.package_feature_link;
