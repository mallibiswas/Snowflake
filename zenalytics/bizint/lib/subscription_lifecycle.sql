-----------------------------------------------------------------------------
---------- Subscription Lifecycle				 ---------
-----------------------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{stageschemaname};

SET ASOF_DATE = (SELECT Current_Date());

create or replace temporary table _d_subscription_lifecycle
as
WITH
future_date as (select last_day(dateadd(year,10,current_date()),year) as fd, to_date('1970-01-01') as pd),
delinquencies as (select recurly_subscription_id,
                        min(subscription_first_realized_date) as subscription_first_realized_date,
                        min(delinquency_begin_date) as delinquency_begin_date,
                        max(case when delinquency_end_date <> future_date.fd then delinquency_end_date else null end) as delinquency_end_date
                        from &{schemaname}.d_delinquency_lifecycle, future_date
                 group by recurly_subscription_id),
post_migration as (select distinct v3_account_id, recurly_subscription_id, migrated_date, subscription_reactivated_date from ams.subscriptions_v2_post_migration)

select  subscription_create_date,
        subscription_update_date,
        state_begin_date,
        state_end_date,
        ts.account_id,
        ts.subscription_id,
        ts.recurly_subscription_id,
        subscription_sk,
        product,
        package,
        plan_code,
        manual_invoice_ind,
        ts.subscription_start_date,
        monthly_subscription_service_fee,
        subscription_active_ind,
        subscription_cancelled_date,
        license_quantity,
        NVL(migrated_date,future_date.pd) as subscription_migrated_date, -- if not migrated account, swt date in the past
        case when m.v3_account_id is not null then NVL(subscription_first_realized_date, subscription_start_date) else NVL(subscription_first_realized_date, future_date.fd) end as subscription_first_realized_date,
--        NVL(subscription_first_realized_date, future_date.fd) as subscription_first_realized_date,
        NVL(delinquency_begin_date, future_date.fd) as delinquency_begin_date,
        NVL(delinquency_end_date, future_date.fd) as delinquency_end_date,
        RANK() over (partition by ts.subscription_id order by subscription_update_date asc) as subscription_lifecycle_number, -- each subs lifecycle, each subs can have multiple lifecycles, all of same subscription id
        DENSE_RANK() over (partition by ts.account_id, ts.subscription_id order by subscription_update_date) as subscription_seq_number, -- sequence of all subscription records within each account
        LAG(monthly_subscription_service_fee) over (partition by ts.subscription_id order by subscription_update_date asc) as prev_monthly_subscription_service_fee,
        LEAD(monthly_subscription_service_fee) over (partition by ts.subscription_id order by subscription_update_date asc) as next_monthly_subscription_service_fee,
        NVL(LAG(license_quantity) over (partition by ts.subscription_id order by subscription_update_date asc),0) as prev_license_quantity,
        NVL(LEAD(license_quantity) over (partition by ts.subscription_id order by subscription_update_date asc),0) as next_license_quantity,
        NVL(LAG(subscription_cancelled_date) over (partition by ts.account_id order by ts.subscription_start_date asc),future_date.fd) as prev_cancellation_date,
        NVL(LEAD(subscription_first_realized_date) over (partition by ts.account_id order by subscription_first_realized_date, subscription_create_date asc),future_date.fd) as next_subscription_realized_date,
        NVL(LEAD(ts.subscription_start_date) over (partition by ts.account_id order by ts.subscription_start_date asc),future_date.fd) as next_subscription_start_date,
        case when m.v3_account_id is not null and ts.subscription_start_date > m.migrated_date then ts.subscription_start_date else m.subscription_reactivated_date end as MNIR_date,         -- add reactivation logic 11/7 for migrated accounts: MNIR = migrated_no_invoice_realized
	case when m.v3_account_id is not null and ts.subscription_start_date > m.migrated_date then True else False end as CNM_fl
from &{schemaname}.subscription_ts ts
left join delinquencies d on ts.recurly_subscription_id = d.recurly_subscription_id
left join post_migration m on m.v3_account_id = ts.account_id and m.recurly_subscription_id = ts.recurly_subscription_id -- added reactivation date logic 11/7
inner join future_date
;

create or replace table d_subscription_lifecycle
as
WITH
future_date as (select last_day(dateadd(year,10,current_date()),year) as fd),
offsetting_churn as (select distinct subscription_id, subscription_cancelled_date from &{schemaname}.d_offsetting_subscriptions s),
offsetting_mvmts as (select distinct offsetting_subscription_id, offsetting_subscription_create_date from &{schemaname}.d_offsetting_subscriptions s)
select
      subscription_create_date,
      subscription_update_date,
      account_id,
      d.subscription_id,
      recurly_subscription_id,
      trim('v3'||':'||d.subscription_id) as subscription_sk,
      subscription_lifecycle_number,
      subscription_seq_number,
      product,
      package,
      plan_code,
      manual_invoice_ind,
      subscription_start_date,
      subscription_migrated_date, -- new field
      monthly_subscription_service_fee,
      subscription_active_ind,
      d.subscription_cancelled_date,
      license_quantity,
      d.subscription_first_realized_date,
      delinquency_begin_date,
      delinquency_end_date,
      case when subscription_lifecycle_number = 1 then LEAST(subscription_start_date,state_begin_date) else state_begin_date end as state_begin_date, -- beginning of a subscription state
      state_end_date, -- each subs can have multiple states
      case when NVL(d.subscription_first_realized_date,future_date.fd) != future_date.fd then True else False end::boolean as realized_fl,
      case when subscription_start_date is not null then True else False end::boolean as booked_fl,
      case when subscription_lifecycle_number > 1 and monthly_subscription_service_fee > prev_monthly_subscription_service_fee then state_begin_date else future_date.fd end as subscription_upgrade_date, -- upgrade = expansion
      case when subscription_lifecycle_number > 1 and monthly_subscription_service_fee < prev_monthly_subscription_service_fee then state_begin_date else future_date.fd end as subscription_downgrade_date,
--      LEAST(delinquency_begin_date, d.subscription_cancelled_date) as churn_begin_date,
--      LEAST(delinquency_end_date, next_subscription_realized_date) as churn_end_date,
      d.subscription_cancelled_date as churn_begin_date, -- take out delinquency from churn logic
      next_subscription_realized_date as churn_end_date, -- take out delinquency from churn logic
      case when delinquency_begin_date < d.subscription_cancelled_date then delinquency_begin_date else future_date.fd end as delinquency_churn_date,
      case when delinquency_begin_date > d.subscription_cancelled_date then d.subscription_cancelled_date else future_date.fd end as subscription_churn_date,
      case when oc.subscription_id is not null and d.subscription_active_ind = False then d.subscription_cancelled_date
	   when om.offsetting_subscription_id is not null then om.offsetting_subscription_create_date
	   else future_date.fd end as subscription_adjustment_date,
      MNIR_date, -- added reactivation date 11/7
      CNM_fl
from _d_subscription_lifecycle d
left join offsetting_churn oc on d.subscription_id = oc.subscription_id
left join offsetting_mvmts om on d.subscription_id = om.offsetting_subscription_id
inner join future_date;

alter table d_subscription_lifecycle add column asof_date date;
update d_subscription_lifecycle set asof_date = $ASOF_DATE;

alter table &{stageschemaname}.d_subscription_lifecycle swap with &{schemaname}.d_subscription_lifecycle;
