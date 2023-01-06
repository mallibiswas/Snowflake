use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Router
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.router as 
select 
$1 as router_id,
$2 as account_id,
$3 as mac_start,
$4 as mac_end,
$5 as router_type,
$6::timestamp as created,
$7::timestamp as updated,
$8::timestamp as deleted,
$9::integer as serial_number,
$10::string as node_id,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/router.csv; 

alter table &{stageschemaname}.router swap with &{schemaname}.router;

--------------------------------------------------------------------
-------------- Router Assignment
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.router_assignment as 
select 
$1 as router_assignment_id,
$2 as router_id,
$3 as business_entity_id,
$4::boolean as dirty,
$5::timestamp as created,
$6::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/router_assignment.csv; 

alter table &{stageschemaname}.router_assignment swap with &{schemaname}.router_assignment;

--------------------------------------------------------------------
-------------- Network Assignment
--------------------------------------------------------------------


create or replace transient table &{stageschemaname}.network_assignment as 
select 
$1 as network_assignment_id,
$2 as business_entity_id,
$3::integer as network_id,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/network_assignment.csv; 

alter table &{stageschemaname}.network_assignment swap with &{schemaname}.network_assignment;
