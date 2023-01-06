
use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Quota 
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.quota as 
select 
$1 as quota_id,
$2::integer as soft_limit,
$3::integer as hard_limit,
$4::number as unit_penalty_in_cents,
$5 as quota_type_id,
$6::timestamp as created,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/quota.csv;

alter table &{stageschemaname}.quota swap with &{schemaname}.quota;
