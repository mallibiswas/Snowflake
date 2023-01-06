use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Feature
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.feature as 
select 
$1 as feature_id,
$2 as name,
$3 as code,
$4::timestamp as created,
$5::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/feature.csv; 

alter table &{stageschemaname}.feature swap with &{schemaname}.feature;
