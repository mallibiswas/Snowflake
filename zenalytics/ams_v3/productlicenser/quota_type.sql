
use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Quota Type
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.quota_type 
as 
select 
$1 as quota_type_id,
$2 as code,
$3 as product_id,
$4::timestamp as created,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/quota_type.csv;

alter table &{stageschemaname}.quota_type swap with &{schemaname}.quota_type;

