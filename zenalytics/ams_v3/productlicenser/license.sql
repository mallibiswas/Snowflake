use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- License
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.license as 
select 
$1 as license_id,
$2 as account_id,
$3 as package_id,
$4::integer as total_units,
$5::integer as assigned_units,
$6::timestamp as created,
$7::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/license.csv; 

alter table &{stageschemaname}.license swap with &{schemaname}.license;

--------------------------------------------------------------------
-------------- License Assignement
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.license_assignment as 
select 
$1 as license_assignment_id,
$2 as license_id,
$3 as business_entity_id,
$4::timestamp as created,
$5::timestamp as updated,
$6::timestamp as deleted,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/license_assignment.csv;

alter table &{stageschemaname}.license_assignment swap with &{schemaname}.license_assignment;
