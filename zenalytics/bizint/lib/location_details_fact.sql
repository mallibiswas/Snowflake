-----------------------------------------------------------------------------
---------- Location Details Fact				 ---------
-----------------------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{stageschemaname};

SET ASOF_DATE = (SELECT Current_Date());

create or replace table location_details_fact
as
with date_dim as (select report_date, report_date_sk
                  from d_date
                  where report_date >= to_date('2018-01-01') and report_date <= current_date())
select report_date,
       account_id,
       location_id,
       case when lower(recurly_plan_name) like '%local%ads%' then 'Local Ads' else 'Core' end as product,
       case when  (report_date between to_date(subscription_first_realized_date) and nvl(to_date(subscription_cancelled_date),report_date)) -- report between realization and cancellation (if exists) 
                  OR report_date > to_date(churn_end_date)  -- after churn reactivation 
                  then True else False end::boolean as active_location_fl,
       case when report_date = to_date(subscription_first_realized_date) and location_lifecycle_number = 1 then True else False end::boolean as new_location_fl,    -- first time realized location           
       case when report_date = to_date(reactivation_date) then True else False end::boolean as reactivation_fl,
       case when report_date = to_date(upgrade_date) then True else False end::boolean as upgrade_fl,
       case when report_date = to_date(downgrade_date) then True else False end::boolean as downgrade_fl,
       case when report_date between to_date(churn_begin_date) and to_date(churn_end_date) then True else False end::boolean as churned_fl,
       case when report_date = to_date(location_expansion_date) then True else False end::boolean as location_expansion_fl, 
       case when report_date = to_date(delinquency_begin_date) then True else False end::boolean as delinquency_churn_fl,
       case when report_date = to_date(subscription_churn_date) then True else False end::boolean as subscription_churn_fl,
       MAX(l.monthly_subscription_service_fee) as monthly_subscription_service_fee
from d_location_lifecycle l, date_dim
where date_dim.report_date >= to_date(subscription_first_realized_date) -- realized subs only
and date_dim.report_date <= nvl(churn_begin_date, date_dim.report_date)
and date_dim.report_date <= migrated_date
and realized_fl = 'Y'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13;


alter table location_details_fact add column asof_date date;
update location_details_fact set asof_date = $ASOF_DATE;


alter table &{stageschemaname}.location_details_fact swap with &{schemaname}.location_details_fact;
