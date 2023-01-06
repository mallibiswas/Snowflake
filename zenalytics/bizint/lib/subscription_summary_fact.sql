-----------------------------------------------------------------------------
---------- Subscription Summary Fact				 ---------
-----------------------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};

SET ASOF_DATE = (SELECT Current_Date());

create or replace table &{stageschemaname}.subscription_summary_fact
as
select  report_date,
        product,
        account_id,
        count(distinct case when active_subscription_fl then subscription_id end) as active_subscriptions,
        sum(license_quantity) as active_licenses,
        SUM(case when active_subscription_fl then monthly_subscription_service_fee else 0 end) as active_subscription_MRR,
        
        count(distinct case when new_realized_fl and not MNIR_fl then subscription_id end) as new_subscriptions,
        SUM(case when new_realized_fl and not MNIR_fl then license_quantity else 0 end) as new_licenses,
        SUM(case when new_realized_fl and not MNIR_fl then monthly_subscription_service_fee else 0 end) as new_subscription_MRR,
        
        count(distinct case when MNIR_fl then subscription_id else null end) as MNIR_subscriptions, -- added reactivation fl 11/7
        SUM(case when MNIR_fl then license_quantity else 0 end) as MNIR_licenses, -- added reactivation fl 11/7
        SUM(case when MNIR_fl then monthly_subscription_service_fee else 0 end) as MNIR_subscription_MRR,

        count(distinct case when subscription_expansion_fl then subscription_id end) as subscription_expansion,
        SUM(case when subscription_expansion_fl then monthly_subscription_service_fee else 0 end) as subscription_expansion_MRR,
        
        count(distinct case when upgrade_fl then subscription_id end) as subscription_upgrades,
        SUM(case when upgrade_fl then monthly_subscription_service_fee else 0 end) as subscription_upgrades_MRR,
        
        count(distinct case when downgrade_fl then subscription_id end) as subscription_downgrades,
        SUM(case when downgrade_fl then monthly_subscription_service_fee else 0 end) as subscription_downgrades_MRR,
        
        count(distinct case when churn_fl then subscription_id end) as churn,
        SUM(case when churn_fl then license_quantity else 0 end) as churn_licenses,
        SUM(case when churn_fl then monthly_subscription_service_fee else 0 end) as churn_MRR,
        
        SUM(case when active_subscription_fl then license_expansion_contraction else 0 end) as license_expansion_contraction,
        SUM(case when active_subscription_fl then mrr_expansion_contraction else 0 end) as mrr_expansion_contraction,
        
        count(distinct case when delinquency_churn_fl then subscription_id end) as delinquency_churns,
        SUM(case when delinquency_churn_fl then monthly_subscription_service_fee else 0 end) as delinquency_churn_MRR,
        
        count(distinct case when subscription_churn_fl then subscription_id end) as subscription_churns,
        SUM(case when subscription_churn_fl then monthly_subscription_service_fee else 0 end) as subscription_churn_MRR
                
from &{schemaname}.subscription_details_fact f
group by rollup(1, 2, 3);

alter table  &{stageschemaname}.subscription_summary_fact add column asof_date date;
update &{stageschemaname}.subscription_summary_fact set asof_date = $ASOF_DATE;

alter table &{stageschemaname}.subscription_summary_fact swap with &{schemaname}.subscription_summary_fact;
