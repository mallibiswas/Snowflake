-----------------------------------------------------------------------------
---------- Location Lifecycle				 ---------
-----------------------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};

SET ASOF_DATE = (SELECT Current_Date());

create or replace temporary table _location_subscriptions
as 
    select  lc.location_id, contract_sk, l.contract_signed_date, l.subscription_sk,  
            l.recurly_subscription_id, l.recurly_plan_name, 
            l.subscription_start_date, l.subscription_activation_date,l.subscription_cancelled_date, 
            l.recurly_location_count, l.monthly_subscription_service_fee 
    from &{sourcedb}.AMS.locationcontract lc 
    INNER JOIN d_order_lifecycle l on lc.contract_id = l.contract_id
    where include_in_reporting = True and l.version = 'v1' and l.contract_signed_date is not null -- location movements only on signed contracts
UNION ALL
    select  lt.location_id, contract_sk, ol.contract_signed_date, subscription_sk, 
            ol.recurly_subscription_id,ol.recurly_plan_name, ol.subscription_start_date, 
            ol.subscription_activation_date,ol.subscription_cancelled_date, 
            ol.recurly_location_count, ol.monthly_subscription_service_fee 
    from &{sourcedb}.AMS.subscriptions_v2_location_through lt
    INNER JOIN d_order_lifecycle ol on lt.subscription_id = ol.subscription_id
    where include_in_reporting = True and version = 'v2' and ol.contract_signed_date is not null;

-- has both signed/realized and signed/unrealized records
create or replace temporary table _d_location_lifecycle
AS
WITH 
location_subscriptions as (select distinct location_id, subscription_sk, recurly_subscription_id, recurly_plan_name, subscription_start_date,
                                        subscription_activation_date, subscription_cancelled_date, recurly_location_count, monthly_subscription_service_fee
                                        from _location_subscriptions
                                        where contract_signed_date is not null
                                        order by location_id),
delinquencies as (select recurly_subscription_id, 
                        min(subscription_first_realized_date) as subscription_first_realized_date,
                        min(delinquency_begin_date) as delinquency_begin_date,
                        max(delinquency_end_date) as delinquency_end_date
                        from d_delinquency_lifecycle
                 group by recurly_subscription_id)
select l.*,d.subscription_first_realized_date,
       delinquency_begin_date,
       delinquency_end_date,       
       RANK() over (partition by location_id order by subscription_start_date asc) as location_lifecycle_number,
       LAG(monthly_subscription_service_fee) over (partition by location_id order by subscription_start_date asc) as prev_monthly_subscription_service_fee,
       LAG(subscription_cancelled_date) over (partition by location_id order by subscription_cancelled_date asc) as prev_cancellation_date,
       LEAD(subscription_first_realized_date) over (partition by location_id order by subscription_cancelled_date asc) as next_subscription_realized_date
from location_subscriptions l LEFT JOIN delinquencies d
on l.recurly_subscription_id = d.recurly_subscription_id;


-- realized subs only
create or replace table d_location_lifecycle
as
WITH
reactivations as (select location_id,location_lifecycle_number,
                  min(case when datediff(day,subscription_cancelled_date, next_subscription_realized_date) <= 7 then next_subscription_realized_date else delinquency_end_date end) as reactivation_date
                  from _d_location_lifecycle
                  where subscription_first_realized_date is not null
                  and next_subscription_realized_date is not null
                  group by location_id,location_lifecycle_number
            ) -- new subs within 7 days of cancellation of old subs)
SELECT l.*,
       case when subscription_first_realized_date is not null then 'Y' else 'N' end as realized_fl,
       reactivation_date,
       case when reactivation_date is null then least(nvl(subscription_cancelled_date,delinquency_begin_date),nvl(delinquency_begin_date,subscription_cancelled_date)) else null end as churn_begin_date,
       case when reactivation_date is null then delinquency_end_date else null end as churn_end_date,
       case when reactivation_date is null and subscription_cancelled_date <= nvl(delinquency_begin_date,subscription_cancelled_date) then subscription_cancelled_date end as subscription_churn_date,
       case when reactivation_date is null and delinquency_begin_date <= nvl(subscription_cancelled_date,delinquency_begin_date) then delinquency_begin_date end as delinquency_churn_begin_date,
       case when reactivation_date is null and delinquency_begin_date <= nvl(subscription_cancelled_date,delinquency_begin_date) then delinquency_end_date end as delinquency_churn_end_date,
       case when monthly_subscription_service_fee > prev_monthly_subscription_service_fee then subscription_first_realized_date else null end as upgrade_date, -- upgraded to higher mrr
       case when monthly_subscription_service_fee < prev_monthly_subscription_service_fee then subscription_first_realized_date else null end as downgrade_date -- downgraded to lower mrr
from _d_location_lifecycle l left join reactivations r
on l.location_id = r.location_id and l.location_lifecycle_number = r.location_lifecycle_number;


alter table d_location_lifecycle add column asof_date date;
update d_location_lifecycle set asof_date = $ASOF_DATE;

