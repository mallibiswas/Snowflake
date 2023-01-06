use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Ads Billing Log
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.ads_billing_log as
select
$1 as ads_billing_log_id,
$2 as campaign_id,
$3::timestamp as start_date,
$4::timestamp as end_date,
$5::integer as total_ads_spend_cents,
$6::integer as total_billed_cents,
$7 as charge_id,
$8 as error,
$9::timestamp as created,
$10::timestamp as updated,
$11::integer as total_spend_with_margin_cents,
$12::integer as total_spend_before_cap_cents,
$13::integer as previous_billed_cents,
$14::integer as io_budget_cents,
$15::integer as billing_month,
to_date('&{asof_date}') as asof_date
FROM @&{stagename}/&{stagepath}/ads_billing_log.csv;

alter table &{stageschemaname}.ads_billing_log swap with &{schemaname}.ads_billing_log;

--------------------------------------------------------------------
-------------- Ads Campaign Daily Spend
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.ads_campaign_daily_spend as
select
$1 as ads_io_id,
$2 as campaign_id,
$3::date as campaign_date,
$4::number as ads_spend_cents,
$5::number as billed_cents,
$6::number(38,2) as margin,
$7::timestamp as created,
$8::timestamp as updated,
to_date('&{asof_date}') as asof_date
FROM @&{stagename}/&{stagepath}/ads_campaign_daily_spend.csv;

alter table &{stageschemaname}.ads_campaign_daily_spend swap with &{schemaname}.ads_campaign_daily_spend;


--------------------------------------------------------------------
-------------- Campaign
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.campaign as
select
$1 as ads_io_id,
$2 as account_id,
$3::date as start_date,
$4::date as end_date,
$5::number as total_price_cents,
$6::number as margin_percent,
$7::boolean as manual_invoice,
$8::boolean as overspend,
$9::boolean as dirty,
$10::timestamp as created,
$11::timestamp as updated,
$12 as internal_description,
to_date('&{asof_date}') as asof_date
FROM @&{stagename}/&{stagepath}/campaign.csv;

alter table &{stageschemaname}.campaign swap with &{schemaname}.campaign;


--------------------------------------------------------------------
-------------- Ads Billing Log Values
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.ads_billing_log_values as
select
$1 as status,
to_date('&{asof_date}') as asof_date
FROM @&{stagename}/&{stagepath}/ads_billing_log_values.csv;

alter table &{stageschemaname}.ads_billing_log_values swap with &{schemaname}.ads_billing_log_values;

--------------------------------------------------------------------
-------------- Ads Billing Log Status
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.ads_billing_log_status as
select
$1 as ads_billing_log_id,
$2 as status,
$3::timestamp as updated,
$4 as note,
$5 as username,
to_date('&{asof_date}') as asof_date
FROM @&{stagename}/&{stagepath}/ads_billing_log_status.csv;

alter table &{stageschemaname}.ads_billing_log_status swap with &{schemaname}.ads_billing_log_status;

--------------------------------------------------------------------
-------------- Ads Billing Log Credit Applied Cents
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.ads_billing_log_credit_applied_cents as
select
$1 as ads_billing_log_id,
$2 as credit_id,
$3::integer as cents,
$4::integer as total_credit_cents,
to_date('&{asof_date}') as asof_date
FROM @&{stagename}/&{stagepath}/ads_billing_log_credit_applied_cents.csv;

alter table &{stageschemaname}.ads_billing_log_credit_applied_cents swap with &{schemaname}.ads_billing_log_credit_applied_cents;

--------------------------------------------------------------------
-------------- Ads Billing Log Credit Percent
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.ads_billing_log_credit_percent as
select
$1 as ads_billing_log_id,
$2 as credit_id,
$3::decimal as value,
$4::integer as cents,
to_date('&{asof_date}') as asof_date
FROM @&{stagename}/&{stagepath}/ads_billing_log_credit_percent.csv;

alter table &{stageschemaname}.ads_billing_log_credit_percent swap with &{schemaname}.ads_billing_log_credit_percent;

--------------------------------------------------------------------
-------------- Ads Billing Log Spend
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.ads_billing_log_spend as
select
$1 as ads_billing_log_id,
$2::integer as spend_cents,
$3::timestamp as date,
$4::float as margin,
$5 as platform_campaign_id,
to_date('&{asof_date}') as asof_date
FROM @&{stagename}/&{stagepath}/ads_billing_log_spend.csv;

alter table &{stageschemaname}.ads_billing_log_spend swap with &{schemaname}.ads_billing_log_spend;
