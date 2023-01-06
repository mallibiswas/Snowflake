-----------------------------------------------------------------------------
---------- Location Details Fact /w Consolidated Billing            ---------
-----------------------------------------------------------------------------


alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};

SET ASOF_DATE = (SELECT Current_Date());

create or replace table location_details_fact
as
with date_dim as (select report_date, report_date_sk 
                  from d_date 
                  where report_date >= to_date('2015-01-01') and report_date <= current_date())
select date_dim.report_date_sk,
       location_id,
       case when lower(recurly_plan_name) like '%local%ads%' then 'Local Ads' else 'Core' end as product,
       'True'::boolean as realized_fl, -- based on filters, all records are selected based on their active periods 
       case when date_dim.report_date = to_date(subscription_cancelled_date) then True else False end::boolean as cancelled_fl, 
       case when date_dim.report_date = to_date(reactivation_date) then True else False end::boolean as subscription_reactivation_fl, 
       case when date_dim.report_date = to_date(upgrade_date) then True else False end::boolean as upgrade_fl, 
       case when date_dim.report_date = to_date(downgrade_date) then True else False end::boolean as downgrade_fl, 
       case when date_dim.report_date between to_date(churn_begin_date) and to_date(churn_end_date) then True else False end::boolean as churned_fl, 
       case when date_dim.report_date = to_date(delinquency_churn_begin_date) then True else False end::boolean as delinquency_churn_fl, 
       case when date_dim.report_date = to_date(churn_end_date) then True else False end::boolean as delinquency_reactivation_fl, 
       case when date_dim.report_date = to_date(subscription_churn_date) then True else False end::boolean as subscription_churn_fl, 
       SUM(l.monthly_subscription_service_fee) as monthly_subscription_service_fee
from d_location_lifecycle l, date_dim
where date_dim.report_date >= to_date(subscription_first_realized_date) -- realized subs only
and date_dim.report_date <= nvl(churn_begin_date, date_dim.report_date)
and realized_fl = 'Y'
group by 1,2,3,4,5,6,7,8,9,10,11,12;


alter table location_details_fact add column asof_date date;
update location_details_fact set asof_date = $ASOF_DATE;


