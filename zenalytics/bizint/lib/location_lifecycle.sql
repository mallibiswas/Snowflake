-----------------------------------------------------------------------------
---------- Location Lifecycle				 ---------
-----------------------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{stageschemaname};

SET ASOF_DATE = (SELECT Current_Date());

create or replace temporary table _location_subscriptions
as
    select  ol.account_id, lc.location_id, contract_sk, ol.contract_signed_date, ol.subscription_sk,
            ol.recurly_subscription_id, ol.recurly_plan_name,
            ol.subscription_start_date, ol.subscription_activation_date, ol.subscription_cancelled_date,
            ol.recurly_location_count, ol.monthly_subscription_service_fee
    from ZENALYTICS.AMS.locationcontract lc
    INNER JOIN ZENALYTICS.AMS.location l on lc.location_id = l.location_id
    INNER JOIN ZENALYTICS.bizint.d_order_lifecycle ol on lc.contract_id = ol.contract_id
    where include_in_reporting = True and ol.version = 'v1' and ol.contract_signed_date is not null and location_state <> 'BETA' -- location movements only on signed contracts
UNION ALL
    select  ol.account_id, lt.location_id, contract_sk, ol.contract_signed_date, subscription_sk,
            ol.recurly_subscription_id,ol.recurly_plan_name, ol.subscription_start_date,
            ol.subscription_activation_date,ol.subscription_cancelled_date,
            ol.recurly_location_count, ol.monthly_subscription_service_fee
    from ZENALYTICS.AMS.subscriptions_v2_location_through lt
    INNER JOIN ZENALYTICS.AMS.location l on lt.location_id = l.location_id
    INNER JOIN d_order_lifecycle ol on lt.subscription_id = ol.subscription_id
    where include_in_reporting = True and version = 'v2' and ol.contract_signed_date is not null and location_state <> 'BETA';


create or replace temporary table _d_location_lifecycle
AS
WITH
future_date as (select last_day(dateadd(year,10,current_date()),year) as fd),
location_subscriptions as (select distinct account_id, location_id, contract_signed_date, subscription_sk, recurly_subscription_id, recurly_plan_name, subscription_start_date,
                                        subscription_activation_date, subscription_cancelled_date, recurly_location_count, monthly_subscription_service_fee
                                        from _location_subscriptions
                                        where contract_signed_date is not null
                                        order by location_id, contract_signed_date),
delinquencies as (select recurly_subscription_id,
                        min(subscription_first_realized_date) as subscription_first_realized_date,
                        min(delinquency_begin_date) as delinquency_begin_date,
                        max(delinquency_end_date) as delinquency_end_date
                        from zenalytics.bizint.d_delinquency_lifecycle
                 group by recurly_subscription_id)
select l.*,
       d.subscription_first_realized_date,
       delinquency_begin_date,
       delinquency_end_date,
       RANK() over (partition by location_id order by subscription_start_date, contract_signed_date, subscription_activation_date asc) as location_lifecycle_number,
       DENSE_RANK() over (partition by account_id order by location_id asc) as location_seq_number,
       LAG(monthly_subscription_service_fee) over (partition by location_id order by subscription_start_date, contract_signed_date, subscription_activation_date asc) as prev_monthly_subscription_service_fee,
       NVL(LAG(subscription_cancelled_date) over (partition by location_id order by subscription_start_date, contract_signed_date, subscription_activation_date asc),future_date.fd) as prev_cancellation_date,
       NVL(LEAD(subscription_first_realized_date) over (partition by location_id order by subscription_start_date, subscription_activation_date asc),future_date.fd) as next_subscription_realized_date,
       NVL(LEAD(subscription_start_date) over (partition by location_id order by subscription_start_date, subscription_activation_date asc),future_date.fd) as next_subscription_start_date,
       NVL(LEAD(contract_signed_date) over (partition by location_id order by contract_signed_date, subscription_activation_date asc),future_date.fd) as next_contract_signed_date
from location_subscriptions l INNER JOIN future_date
LEFT JOIN delinquencies d
on l.recurly_subscription_id = d.recurly_subscription_id;


create or replace table d_location_lifecycle
as
WITH
future_date as (select last_day(dateadd(year,10,current_date()),year) as fd),
continuations as (select location_id,location_lifecycle_number,
--                  MIN(case when datediff(day,subscription_cancelled_date, next_subscription_realized_date) <= 7 then next_subscription_realized_date else delinquency_end_date end) as continuation_date
                  MIN(case when datediff(day,subscription_cancelled_date, next_contract_signed_date) <= 7 then next_subscription_start_date else delinquency_end_date end) as continuation_date
                  from _d_location_lifecycle, future_date
                  where subscription_first_realized_date is not null
                  and next_subscription_start_date is not null -- needs to start, not necessarily realized
                  group by location_id,location_lifecycle_number
            ) -- new subs start within 7 days of cancellation of old subs)
SELECT  l.account_id,
        NVL(ac.migrated_date,future_date.fd) as migrated_date,
        l.location_id,
        l.subscription_sk,
        l.recurly_subscription_id,
        l.recurly_plan_name,
        l.contract_signed_date,
        l.subscription_start_date,
        l.subscription_activation_date,
        l.subscription_cancelled_date,
        l.recurly_location_count,
        l.monthly_subscription_service_fee,
        l.subscription_first_realized_date,
        l.delinquency_begin_date,
        l.location_lifecycle_number,
       continuation_date,
       case when l.subscription_first_realized_date is not null then True else False end::boolean as realized_fl,
       case when l.subscription_start_date is not null then True else False end::boolean as booked_fl,
       case when l.location_seq_number > 1 and l.location_lifecycle_number = 1 then subscription_first_realized_date else future_date.fd end as location_expansion_date,
       case when nvl(continuation_date,future_date.fd) = future_date.fd then least(nvl(subscription_cancelled_date,delinquency_begin_date),nvl(delinquency_begin_date,subscription_cancelled_date)) else future_date.fd end as churn_begin_date,
       case when nvl(continuation_date,future_date.fd) = future_date.fd then delinquency_end_date else future_date.fd end as churn_end_date,
       case when nvl(continuation_date,future_date.fd) = future_date.fd and subscription_cancelled_date <= nvl(delinquency_begin_date,subscription_cancelled_date) then subscription_cancelled_date else future_date.fd end as subscription_churn_date,
       case when nvl(continuation_date,future_date.fd) = future_date.fd and delinquency_begin_date <= nvl(subscription_cancelled_date,delinquency_begin_date) then delinquency_begin_date else future_date.fd end as delinquency_churn_date,
       case when monthly_subscription_service_fee > prev_monthly_subscription_service_fee then subscription_first_realized_date else future_date.fd end as upgrade_date, -- upgraded to higher mrr
       case when monthly_subscription_service_fee < prev_monthly_subscription_service_fee then subscription_first_realized_date else future_date.fd end as downgrade_date, -- downgraded to lower mrr
       case when monthly_subscription_service_fee <> prev_monthly_subscription_service_fee then (monthly_subscription_service_fee-prev_monthly_subscription_service_fee) else 0 end as MRR_Change_amount, -- downgraded to lower mrr
       case when l.location_lifecycle_number = 1 then future_date.fd
            when l.location_lifecycle_number > 1 and NVL(LAG(continuation_date) over (partition by l.location_id order by l.location_lifecycle_number asc),future_date.fd) = future_date.fd then subscription_first_realized_date else future_date.fd end as reactivation_date
from _d_location_lifecycle l inner join zenalytics.ams.account ac on l.account_id = ac.account_id
left join continuations r on l.location_lifecycle_number = r.location_lifecycle_number and l.location_id = r.location_id
inner join future_date;

alter table d_location_lifecycle add column asof_date date;
update d_location_lifecycle set asof_date = $ASOF_DATE;

alter table &{stageschemaname}.d_location_lifecycle swap with &{schemaname}.d_location_lifecycle;
