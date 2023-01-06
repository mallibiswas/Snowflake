use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Recurly Plan
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.recurly_plan as 
select
$1 as plan_id,
$2 as plan_code,
$3 as payment_frequency_months,
$4::timestamp as created,
$5::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/recurly_plan.csv; 

alter table &{stageschemaname}.recurly_plan swap with &{schemaname}.recurly_plan;

--------------------------------------------------------------------
-------------- Recurly Provider
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.recurly_provider as 
select
$1 as provider_id,
$2 as recurly_id,
$3 as name,
$4 as email,
$5 as url,
$6::timestamp as created,
$7::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/recurly_provider.csv; 

alter table &{stageschemaname}.recurly_provider swap with &{schemaname}.recurly_provider;

--------------------------------------------------------------------
-------------- Recurly Subscriptions
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.recurly_subscription as 
select
$1 as recurly_subscription_key,
$2 as recurly_subscription_id,
$3::timestamp as start_date,
$4 as unit_price_cents,
$5::boolean as active,
$6::integer as quantity,
$7 as collection_method,
$8 as notes,
$9 as url,
$10 as plan_code,
$11::integer as billing_frequency_months,
$12::timestamp as created,
$13::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/recurly_subscription.csv; 

alter table &{stageschemaname}.recurly_subscription swap with &{schemaname}.recurly_subscription;

--------------------------------------------------------------------
-------------- Recurly Subscription Snapshot
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.recurly_subscription_snapshot as 
select
$1 as recurly_subscription_snapshot_id,
$2 as recurly_subscription_id,
$3::timestamp as start_date,
$4 as unit_price_cents,
$5::boolean as active,
$6::integer as quantity,
$7 as collection_method,
$8 as notes,
$9 as url,
$10 as plan_code,
$11::integer as billing_frequency_months,
$12::timestamp as created,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/recurly_subscription_snapshot.csv; 

alter table &{stageschemaname}.recurly_subscription_snapshot swap with &{schemaname}.recurly_subscription_snapshot;
