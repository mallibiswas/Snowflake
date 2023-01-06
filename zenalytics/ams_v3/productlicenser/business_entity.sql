use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Business Entity
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.business_entity as 
select 
$1 as business_entity_id,
$2 as parent_id,
$3 as name,
$4 as account_id,
$5 as crm_id,
$6 as salesforce_id,
$7 as type,
$8::timestamp as created,
$9::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/business_entity.csv; 

alter table &{stageschemaname}.business_entity swap with &{schemaname}.business_entity;

