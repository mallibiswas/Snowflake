use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Account
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.account as 
select 
$1 as account_id,
$2 as payment_info_id,
$3 as salesforce_account_id,
$4 as account_type,
$5::boolean as active,
$6::timestamp as created,
$7::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/account.csv;

alter table &{stageschemaname}.account swap with &{schemaname}.account; 

create or replace transient table &{stageschemaname}.account_payment_infos as
select
$1 as account_payment_infos_key,
$2 as payment_info_id,
$3 as account_id,
$4::timestamp as created,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/account_payment_infos.csv
;

alter table &{stageschemaname}.account_payment_infos swap with &{schemaname}.account_payment_infos; 
