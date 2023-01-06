{{
    config(
        materialized='incremental',
        unique_key='subscription_detail_id'
    )
}}

--_subscription_details_fact
WITH date_dim as (
    select report_date,
           report_date_sk
    from {{ ref('mart_bizint__d_date') }}
    where report_date >= to_date('2015-01-01')
        and report_date <= current_date()
), future_date as (
    select last_day(dateadd(year,10,current_date()),year) as fd
), _subscription_details_fact as (
    select date_dim.report_date,
           account_id,
           subscription_id,
           subscription_migrated_date,
           Product,
           case when (date_dim.report_date between to_date(subscription_first_realized_date) and to_date(churn_begin_date)) then true else false end as active_subscription_fl,
           case when date_dim.report_date = to_date(subscription_first_realized_date) then true else False end as new_realized_fl,
           case when date_dim.report_date = to_date(subscription_cancelled_date) then True else False end::boolean as cancelled_fl,
           case when date_dim.report_date = to_date(subscription_upgrade_date) then True else False end::boolean as upgrade_fl,
           case when date_dim.report_date = to_date(subscription_downgrade_date) then True else False end::boolean as downgrade_fl,
           case when date_dim.report_date = to_date(churn_begin_date) then True else False end::boolean as churn_fl,
           case when report_date = to_date(subscription_upgrade_date) then True else False end::boolean as subscription_expansion_fl,
           case when report_date = to_date(delinquency_begin_date) then True else False end::boolean as delinquency_churn_fl,
           case when report_date = to_date(subscription_churn_date) then True else False end::boolean as subscription_churn_fl,
           case when report_date = to_date(MNIR_date) then True else False end::boolean as MNIR_fl, -- added reactivation flag 11/7
           case when report_date = to_date(subscription_adjustment_date) then True else False end::boolean as subscription_adjustment_fl,
           monthly_subscription_service_fee,
           license_quantity
    from {{ ref('mart_bizint__d_subscription_lifecycle') }} l,
         date_dim,
         future_date
    where date_dim.report_date >= to_date(subscription_first_realized_date) -- realized subs only
        and date_dim.report_date <= nvl(to_date(churn_begin_date),report_date)
        and nvl(to_date(churn_begin_date),future_date.fd) <= future_date.fd
        and date_dim.report_date >= to_date(state_begin_date)  -- bound each subscription values to within it's valid dates
        and date_dim.report_date < to_date(state_end_date)
        and date_dim.report_date >= to_date(subscription_migrated_date) -- since migration or new subs only
)
SELECT account_id || '|' || SUBSCRIPTION_ID || '|' || (TO_CHAR(REPORT_DATE, 'YYYYMMDD')) as subscription_detail_id,
       REPORT_DATE,
       ACCOUNT_ID,
       SUBSCRIPTION_ID,
       SUBSCRIPTION_MIGRATED_DATE,
       PRODUCT,
       ACTIVE_SUBSCRIPTION_FL,
       NEW_REALIZED_FL,
       CANCELLED_FL,
       UPGRADE_FL,
       DOWNGRADE_FL,
       CHURN_FL,
       SUBSCRIPTION_EXPANSION_FL,
       DELINQUENCY_CHURN_FL,
       SUBSCRIPTION_CHURN_FL,
       MNIR_FL,
       SUBSCRIPTION_ADJUSTMENT_FL,
       MONTHLY_SUBSCRIPTION_SERVICE_FEE,
       MONTHLY_SUBSCRIPTION_SERVICE_FEE -
       NVL(LAG(MONTHLY_SUBSCRIPTION_SERVICE_FEE) OVER (PARTITION BY SUBSCRIPTION_ID ORDER BY REPORT_DATE),0)            AS MRR_EXPANSION_CONTRACTION,
       LICENSE_QUANTITY,
       LICENSE_QUANTITY - NVL(LAG(LICENSE_QUANTITY) OVER (PARTITION BY SUBSCRIPTION_ID ORDER BY REPORT_DATE),0)         AS LICENSE_EXPANSION_CONTRACTION,
       current_date as ASOF_DATE
FROM _subscription_details_fact
{% if is_incremental() %}
    where report_date >= (SELECT dateadd(day,-14,to_date((select max(report_date) from {{ this }} ))))
{% endif %}