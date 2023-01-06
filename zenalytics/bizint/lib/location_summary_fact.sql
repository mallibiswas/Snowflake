-----------------------------------------------------------------------------
---------- Location Summary Fact				 ---------
-----------------------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';



use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{stageschemaname};

SET ASOF_DATE = (SELECT Current_Date());

create or replace table location_summary_fact
as
select  report_date, 
        product,
        account_id,
        count(distinct case when active_location_fl then location_id end) as active_locations,
        SUM(case when active_location_fl then monthly_subscription_service_fee else 0 end) as active_locations_MRR,
        count(distinct case when new_location_fl then location_id end) as new_locations,
        SUM(case when new_location_fl then monthly_subscription_service_fee else 0 end) as new_locations_MRR,
        count(distinct case when location_expansion_fl then location_id end) as location_expansion,  
        SUM(case when location_expansion_fl then monthly_subscription_service_fee else 0 end) as expansion_locations_MRR,  
        count(distinct case when reactivation_fl then location_id end) as reactivations,
        SUM(case when reactivation_fl then monthly_subscription_service_fee else 0 end) as reactivations_MRR,
        count(distinct case when upgrade_fl then location_id end) as upgrades,
        SUM(case when upgrade_fl then monthly_subscription_service_fee else 0 end) as upgrades_MRR,
        count(distinct case when downgrade_fl then location_id end) as downgrades,
        SUM(case when downgrade_fl then monthly_subscription_service_fee else 0 end) as downgrades_MRR,
        count(distinct case when churned_fl then location_id end) as churn,
        SUM(case when churned_fl then monthly_subscription_service_fee else 0 end) as churn_MRR,
        count(distinct case when delinquency_churn_fl then location_id end) as delinquency_churns,
        SUM(case when delinquency_churn_fl then monthly_subscription_service_fee else 0 end) as delinquency_churn_MRR,
        count(distinct case when subscription_churn_fl then location_id end) as subscription_churns,
        SUM(case when subscription_churn_fl then monthly_subscription_service_fee else 0 end) as subscription_churn_MRR
from location_details_fact f
group by rollup(1, 2, 3);

alter table location_summary_fact add column asof_date date;
update location_summary_fact set asof_date = $ASOF_DATE;

alter table &{stageschemaname}.location_summary_fact swap with &{schemaname}.location_summary_fact;
