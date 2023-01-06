use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Logical Router
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.logical_router as 
select 
$1 as logical_router_id,
$2 as router_id,
$3 as mac,
$4::timestamp as created_in_crm,
$5::timestamp as created,
$6::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/logical_router.csv; 

alter table &{stageschemaname}.logical_router swap with &{schemaname}.logical_router;

--------------------------------------------------------------------
-------------- Logical Router Assignment
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.logical_router_assignment as 
select 
$1 as logical_router_assignment_id,
$2 as logical_router_id,
$3 as router_assignment_id,
$4::timestamp as assigned_in_crm,
$5::timestamp as unassigned_in_crm,
$6::timestamp as created,
$7::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/logical_router_assignment.csv; 

alter table &{stageschemaname}.logical_router_assignment swap with &{schemaname}.logical_router_assignment;
