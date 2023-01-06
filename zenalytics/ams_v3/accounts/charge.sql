use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Charge
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.charge as
select
$1 as charge_key,
$2 as account_id,
$3 as charge_id,
$4 as name,
$5::integer as quantity,
$6::integer as unit_price_cents,
$7::timestamp as created,
$8::timestamp as updated,
to_date('&{asof_date}') as asof_date
FROM @&{stagename}/&{stagepath}/charge.csv;

alter table &{stageschemaname}.charge swap with &{schemaname}.charge;

--------------------------------------------------------------------
-------------- Charge Log
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.charge_log as
select
$1 as charge_log_id,
$2 as account_id,
$3 as salesforce_quote_line_item_key,
$4 as charge_id,
$5::integer as unit_amount_in_cents,
$6::integer as quantity,
$7 as description,
$8 as error,
$9::timestamp as created,
$10::timestamp as updated,
to_date('&{asof_date}') as asof_date
FROM @&{stagename}/&{stagepath}/charge_log.csv;

alter table &{stageschemaname}.charge_log swap with &{schemaname}.charge_log;
