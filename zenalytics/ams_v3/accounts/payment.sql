use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- payment Info
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.payment_info as 
select
$1 as payment_id,
$2 as provider_type,
$3 as recurly_provider_id,
$4::timestamp as created,
$5::timestamp as updated,
$6 as type,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/payment_info.csv; 

alter table &{stageschemaname}.payment_info swap with &{schemaname}.payment_info;
