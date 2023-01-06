use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Subscription
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.subscription as 
select
$1 as subscription_id,
$2 as account_id,
$3 as recurly_subscription_key,
$4 as provider_type,
$5 as product,
$6 as package,
$7::boolean as manual_invoice,
$8::timestamp as created,
$9::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/subscription.csv; 

alter table &{stageschemaname}.subscription swap with &{schemaname}.subscription;

--------------------------------------------------------------------
-------------- Subscription Log
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.subscription_log as 
select
$1 as subscription_log_id,
$2 as account_id,
$3 as salesforce_quote_line_item_key,
$4 as subscription_id,
$5::boolean as active,
$6 as product,
$7 as package,
$8::boolean as manual_invoice,
$9::integer as unit_price_cents,
$10::integer as quantity,
$11::timestamp as start_date,
$12::integer as billing_frequency_months,
$13 as notes,
$14 as operation,
$15 as error,
$16::timestamp as created,
$17::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/subscription_log.csv; 

alter table &{stageschemaname}.subscription_log swap with &{schemaname}.subscription_log;

--------------------------------------------------------------------
-------------- Subscription Snapshot
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.subscription_snapshot as 
select
$1 as subscription_snapshot_id,
$2 as subscription_id,
$3 as recurly_subscription_snapshot_id,
$4 as account_id,
$5 as recurly_subscription_id,
$6 as provider_type,
$7 as product,
$8 as package,
$9::boolean as manual_invoice,
$10::timestamp as created,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/subscription_snapshot.csv; 

alter table &{stageschemaname}.subscription_snapshot swap with &{schemaname}.subscription_snapshot;
