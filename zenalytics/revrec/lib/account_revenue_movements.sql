alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};

-- Schema to execute script on: zenalytics.revrec

------------------------------------------------------------------
-- Changelog:
-- [MB] 10/20: to track Hardware revenue, delete "WHERE adjustment_product_category IN ('Core', 'Ads');" from d_invoice_adjustments
-- [MB] 10/21: Remove "WHERE  mrr_delta != 0" from revenue movements fact to be able to calculate "Existing" revenue in the report
-- [MB] 10/22: Added Invoice State to d_invoice_adjustments. Added ads_budget_monthly_fact and ads_spend_monthly_fact tables
-- [MB] 11/12: Added prorated budget and total budget to ads_budget_monthly_fact
-- [MB] 11/19: Update to serice dates adjectments logic to control for movements due to future renewals
-- [MB] 11/19: Update ads budget fact table to exclude test campaign budgets
------------------------------------------------------------------

-------------------------------------------------------------------
--- invoice adjustments
-------------------------------------------------------------------

create or replace table d_invoice_adjustments
as
WITH
invoices AS (
  SELECT
    *
  FROM
    recurly.invoices
  ORDER BY
     account
    ,number
),
parsed_rows AS (
  SELECT
     invoices.*
    ,flattened_adjustments.value parsed_adjustment
  FROM
     invoices
    ,table(flatten(parse_json(LINE_ITEMS):data)) flattened_adjustments
)
SELECT
     parse_json(parsed_rows.ACCOUNT):code::text          account_code
    ,parse_json(parsed_rows.ACCOUNT):company::text       account_company
    ,parsed_adjustment:subscription_id::text             adjustment_subscription_id
    ,parsed_rows.number                                  invoice_number
    ,parsed_rows.collection_method                       invoice_collection_method
    ,parsed_rows.origin                                  invoice_origin
    ,parsed_rows.state                                   invoice_state
    ,parsed_rows.discount                                invoice_discount
    ,parsed_rows.subtotal                                invoice_subtotal
    ,parsed_rows.total                                   invoice_total
    ,parsed_rows.paid                                    invoice_paid
    ,parsed_rows.balance                                 invoice_balance
    ,parsed_rows.created_at                              invoice_date_created
    ,parsed_rows.closed_at                               invoice_date_closed
    ,parsed_adjustment:invoice_href::text                adjustment_invoice_id
    ,parsed_adjustment:state::text                       adjustment_state
    ,parsed_adjustment:accounting_code::text             adjustment_accounting_code
    ,parsed_adjustment:revenue_schedule_type::text       adjustment_revenue_schedule_type
    ,parsed_adjustment:currency::text                    adjustment_currency
    ,parsed_adjustment:description::text                 adjustment_description
    ,parsed_adjustment:created_at::TIMESTAMP             adjustment_account_created_at
    ,parsed_adjustment:updated_at::TIMESTAMP             adjustment_updated_at
    ,parsed_adjustment:uuid::text                        adjustment_uuid
    ,parsed_adjustment:origin::text                      adjustment_origin
    ,parsed_adjustment:refund::text                      adjustment_refund
    ,parsed_adjustment:product_code::text                adjustment_product_code
    ,TO_DATE(parsed_adjustment:start_date::TIMESTAMP)    adjustment_start_date
    ,TO_DATE(parsed_adjustment:end_date::TIMESTAMP)      adjustment_end_date
    ,parsed_adjustment:taxable::text                     adjustment_taxable
    ,parsed_adjustment:tax_in_cents::DECIMAL             adjustment_tax_in_cents
    ,parsed_adjustment:unit_amount::DECIMAL              adjustment_unit_amount
    ,parsed_adjustment:quantity::INTEGER                 adjustment_quantity
    ,parsed_adjustment:subtotal::DECIMAL                 adjustment_subtotal
    ,parsed_adjustment:discount::DECIMAL                 adjustment_discount
    ,parsed_adjustment:amount::DECIMAL                   adjustment_amount
    ,CASE
        WHEN lower(adjustment_description)  LIKE '%hardware%'                  THEN 'Hardware'
        WHEN lower(adjustment_description)  LIKE '%installation fee%'          THEN 'Installation'
        WHEN lower(adjustment_description)  LIKE '%intallation fee%'           THEN 'Installation'
        WHEN lower(adjustment_description)  LIKE '%ads%'                       THEN 'Ads'
        WHEN lower(adjustment_product_code) LIKE '%ads%'                       THEN 'Ads'
        WHEN lower(adjustment_description)  LIKE '%attract%'                   THEN 'Ads'
        WHEN lower(adjustment_product_code) LIKE '%attract%'                   THEN 'Ads'
        WHEN lower(adjustment_description)  LIKE '%variable ad spend%'         THEN 'Ads'
        WHEN lower(adjustment_description)  LIKE '%promotion%'                 THEN 'Ads'
        WHEN lower(adjustment_description)  LIKE '%advertising by zenreach%'   THEN 'Ads'
                                                                                ELSE 'Core'
    END as adjustment_product_category
    , CASE
        WHEN adjustment_subscription_id IS NOT NULL                            THEN 'Subscription'
                                                                                ELSE 'One-time'
        END AS adjustment_invoice_type
    ,CASE
        WHEN account_code = '8a2f606a-ed45-47cb-bd9e-13458296ef11' AND adjustment_invoice_type = 'One-time' AND adjustment_product_category = 'Ads' THEN 'Arrears' -- Postmates
        WHEN ams_accounts.ads_billing_log.charge_id IS NULL                     THEN 'Advance'
                                                                                ELSE 'Arrears'
        END AS billing_timing
    ,CASE
        WHEN billing_timing = 'Advance'                                         THEN adjustment_product_category
        WHEN billing_timing = 'Arrears'                                         THEN 'Ads IO'
                                                                                ELSE 'Unknown'
        END AS adjustment_product_family
    ,CASE
        WHEN (billing_timing = 'Arrears')                                       THEN adjustment_end_date + 1
                                                                                ELSE adjustment_end_date
        END AS adjustment_end_date_adjusted
    , recurly.plans.interval_length__it
    , DATEDIFF(DAY, adjustment_start_date, adjustment_end_date)                adjustment_length_days
    , DATEDIFF(MONTH, adjustment_start_date, adjustment_end_date)              adjustment_length_months
    , CASE
        WHEN adjustment_length_days <= 31                                      THEN 1
        WHEN adjustment_length_days > 31 AND adjustment_length_days <= 62     THEN 2
        WHEN adjustment_length_days > 62 AND adjustment_length_days <= 92     THEN 3
                                                                                ELSE adjustment_length_months
     END AS adjustment_length_days_backfill
    , NVL(recurly.plans.interval_length__it, adjustment_length_days_backfill)    adjustment_months
    , adjustment_amount/ NVL(adjustment_months,1)                             adjustment_mrr
    , MIN(parsed_adjustment:start_date::TIMESTAMP) OVER (PARTITION BY account_code, adjustment_product_category) earliest_adjustment_start
  FROM
    parsed_rows
  LEFT JOIN recurly.plans
         ON recurly.plans.code = parsed_adjustment:product_code::text
  LEFT JOIN ams_accounts.ads_billing_log
         ON ams_accounts.ads_billing_log.charge_id = parsed_adjustment:uuid::text
;

alter table d_invoice_adjustments add column asof_date datetime;
update d_invoice_adjustments set asof_date=current_timestamp;

--------------------------------------------------------
---------------------------- temporary object to hold array subset values
--------------------------------------------------------

create or replace temporary table array_subset
AS
WITH
array_values_sub AS(
SELECT
     sfdc_a.Name as adjustment_sub_sfdc_name
    ,ams_accounts.subscription.account_id as ams_account_id
    ,recurly.subscriptions.expires_at
    ,d_invoice_adjustments.*
FROM
     d_invoice_adjustments

LEFT JOIN recurly.subscriptions
       ON recurly.subscriptions.id = d_invoice_adjustments.adjustment_subscription_id

LEFT JOIN ams_accounts.recurly_subscription
       ON ams_accounts.recurly_subscription.recurly_subscription_id = recurly.subscriptions.uuid

LEFT JOIN ams_accounts.subscription
       ON ams_accounts.subscription.recurly_subscription_key = ams_accounts.recurly_subscription.recurly_subscription_key

LEFT JOIN ams_accounts.account
       ON ams_accounts.account.account_id = ams_accounts.subscription.account_id

LEFT JOIN ams_accounts.salesforce_account
       ON ams_accounts.salesforce_account.salesforce_account_id = ams_accounts.account.salesforce_account_id

LEFT JOIN SFDC.ACCOUNT sfdc_a
       ON sfdc_a.id = ams_accounts.salesforce_account.salesforce_account_id
),

billing_provider AS (
SELECT
         ams_accounts.recurly_provider.email
        ,ams_accounts.recurly_provider.name                             recurly_provider_name
        ,ams_accounts.recurly_provider.provider_id
        ,ams_accounts.recurly_provider.recurly_id
        ,ams_accounts.recurly_provider.url

        ,ams_accounts.payment_info.payment_id
        ,ams_accounts.payment_info.provider_type
        ,ams_accounts.payment_info.recurly_provider_id
        ,ams_accounts.payment_info.type                                 payment_info_type

        ,ams_accounts.account_payment_infos.account_id                  payment_infos_account_id
        ,ams_accounts.account_payment_infos.account_payment_infos_key
        ,ams_accounts.account_payment_infos.payment_info_id             payment_infos_payment_info_id

        ,ams_accounts.account.account_id                                billing_account_id
        ,ams_accounts.account.account_type                              billing_account_type
        ,ams_accounts.account.active                                    billing_active
        ,ams_accounts.account.payment_info_id
        ,ams_accounts.account.salesforce_account_id

        ,ams_accounts.salesforce_account.name
FROM
    ams_accounts.recurly_provider
LEFT JOIN ams_accounts.payment_info
       ON ams_accounts.payment_info.recurly_provider_id = ams_accounts.recurly_provider.provider_id
LEFT JOIN ams_accounts.account_payment_infos
       ON ams_accounts.account_payment_infos.payment_info_id = ams_accounts.payment_info.payment_id
LEFT JOIN ams_accounts.account
       ON ams_accounts.account.account_id = ams_accounts.account_payment_infos.account_id
LEFT JOIN ams_accounts.salesforce_account
       ON ams_accounts.salesforce_account.salesforce_account_id = ams_accounts.account.salesforce_account_id
)

SELECT array_values_sub.ACCOUNT_CODE
      , array_values_sub.ACCOUNT_COMPANY
      , array_values_sub.ADJUSTMENT_ACCOUNT_CREATED_AT
      , array_values_sub.ADJUSTMENT_ACCOUNTING_CODE
      , array_values_sub.ADJUSTMENT_AMOUNT
      , array_values_sub.ADJUSTMENT_CURRENCY
      , array_values_sub.ADJUSTMENT_DESCRIPTION
      , array_values_sub.ADJUSTMENT_DISCOUNT
      , array_values_sub.ADJUSTMENT_END_DATE
      , array_values_sub.ADJUSTMENT_END_DATE_ADJUSTED
      , array_values_sub.ADJUSTMENT_INVOICE_ID
      , array_values_sub.ADJUSTMENT_INVOICE_TYPE
      , array_values_sub.ADJUSTMENT_LENGTH_DAYS
      , array_values_sub.ADJUSTMENT_LENGTH_DAYS_BACKFILL
      , array_values_sub.ADJUSTMENT_LENGTH_MONTHS
      , array_values_sub.ADJUSTMENT_MONTHS
      , array_values_sub.ADJUSTMENT_MRR
      , array_values_sub.ADJUSTMENT_ORIGIN
      , array_values_sub.ADJUSTMENT_PRODUCT_CATEGORY
      , array_values_sub.ADJUSTMENT_PRODUCT_CODE
      , array_values_sub.ADJUSTMENT_PRODUCT_FAMILY
      , array_values_sub.ADJUSTMENT_QUANTITY
      , array_values_sub.ADJUSTMENT_REFUND
      , array_values_sub.ADJUSTMENT_REVENUE_SCHEDULE_TYPE
      , array_values_sub.ADJUSTMENT_START_DATE
      , array_values_sub.ADJUSTMENT_STATE
      , array_values_sub.ADJUSTMENT_SUB_SFDC_NAME
      , array_values_sub.ADJUSTMENT_SUBSCRIPTION_ID
      , array_values_sub.ADJUSTMENT_SUBTOTAL
      , array_values_sub.ADJUSTMENT_TAX_IN_CENTS
      , array_values_sub.ADJUSTMENT_TAXABLE
      , array_values_sub.ADJUSTMENT_UNIT_AMOUNT
      , array_values_sub.ADJUSTMENT_UPDATED_AT
      , array_values_sub.ADJUSTMENT_UUID
      , array_values_sub.BILLING_TIMING
      , array_values_sub.EARLIEST_ADJUSTMENT_START
      , array_values_sub.EXPIRES_AT
      , array_values_sub.INTERVAL_LENGTH__IT
      , array_values_sub.INVOICE_BALANCE
      , array_values_sub.INVOICE_COLLECTION_METHOD
      , array_values_sub.INVOICE_DATE_CLOSED
      , array_values_sub.INVOICE_DATE_CREATED
      , array_values_sub.INVOICE_DISCOUNT
      , array_values_sub.INVOICE_NUMBER
      , array_values_sub.INVOICE_ORIGIN
      , array_values_sub.INVOICE_PAID
      , array_values_sub.INVOICE_SUBTOTAL
      , array_values_sub.INVOICE_TOTAL
      , billing_provider.recurly_provider_name
      , billing_provider.billing_account_id
      , billing_provider.recurly_id
      , NVL(array_values_sub.ams_account_id, billing_provider.billing_account_id) ams_account_id
      , NVL(array_values_sub.adjustment_sub_sfdc_name, billing_provider.name) account_name
FROM
    array_values_sub
LEFT JOIN billing_provider
       ON (billing_provider.recurly_id = array_values_sub.account_code AND array_values_sub.adjustment_subscription_id IS NULL)
;


--------------------------------------------
--- Create account milestones service dates
--------------------------------------------

create or replace table d_account_service_dates
as
SELECT
    ams_account_id
    ,account_name
    ,adjustment_product_category
    ,adjustment_product_family
    ,billing_timing
    ,movement_date
    ,MIN(movement_date) OVER (PARTITION BY ams_account_id, adjustment_product_category) AS earliest_start
    ,MAX(movement_date) OVER (PARTITION BY ams_account_id, adjustment_product_category) AS final
FROM
    (
    SELECT
        ams_account_id
        ,account_name
        ,adjustment_product_category
        ,adjustment_product_family
        ,billing_timing
        ,adjustment_start_date movement_date
    FROM
        array_subset
    WHERE
        ams_account_id IS NOT NULL
    GROUP BY
        ams_account_id
        ,account_name
        ,adjustment_product_category
        ,adjustment_product_family
        ,billing_timing
        ,adjustment_start_date
    UNION ALL
    SELECT
        ams_account_id
        ,account_name
        ,adjustment_product_category
        ,adjustment_product_family
        ,billing_timing
        ,adjustment_end_date_adjusted movement_date
    FROM
        array_subset
    WHERE
        ams_account_id IS NOT NULL
    GROUP BY
        ams_account_id
        ,account_name
        ,adjustment_product_category
        ,adjustment_product_family
        ,billing_timing
        ,adjustment_end_date_adjusted
    )
GROUP BY
    ams_account_id
    ,account_name
    ,adjustment_product_category
    ,adjustment_product_family
    ,billing_timing
    ,movement_date
ORDER BY
     adjustment_product_category
    ,adjustment_product_family
    ,billing_timing
    ,ams_account_id
    ,account_name
    ,movement_date
;

alter table d_account_service_dates add column asof_date datetime;
update d_account_service_dates set asof_date=current_timestamp;

------------------------------------
--- service dates adjustments
-----------------------------------

create or replace table d_service_dates_adjustments
AS
SELECT
     d.adjustment_product_category
    ,d.adjustment_product_family
    ,d.ams_account_id
    ,d.account_name
    ,d.earliest_start
    ,d.final
    ,d.movement_date
    ,d.billing_timing -- Temporary
    ,array_subset.expires_at
    ,array_subset.adjustment_start_date
    ,array_subset.adjustment_end_date
    ,DATE_TRUNC('month', ADD_MONTHS(CURRENT_DATE(), -1)) MD_Temporary --Temporary
    ,CASE
        -- Subscriptions billed In Advance will be considered Churn once last ED is expired
        WHEN (d.billing_timing = 'Advance') AND (array_subset.adjustment_end_date IS NULL) AND (d.movement_date < CURRENT_DATE()) THEN 0
        WHEN (d.billing_timing = 'Arrears') AND (array_subset.adjustment_end_date IS NULL) AND (d.movement_date < DATE_TRUNC('month', ADD_MONTHS(CURRENT_DATE(), -1))) THEN 0
        ELSE array_subset.adjustment_amount
        END AS subtotal_amount
    ,CASE
        WHEN (array_subset.adjustment_end_date =  movement_date) AND (array_subset.adjustment_end_date >= array_subset.expires_at) THEN 0
        ELSE array_subset.adjustment_mrr
        END AS mrr
FROM
    d_account_service_dates d
LEFT JOIN array_subset
       ON array_subset.ams_account_id                 = d.ams_account_id
      AND array_subset.adjustment_product_category    =  d.adjustment_product_category
      AND array_subset.adjustment_product_family      =  d.adjustment_product_family
      AND (
           (    array_subset.adjustment_start_date         <=  movement_date
            AND array_subset.adjustment_end_date           >   movement_date -- one exception would be if it has a start and end date of the same day. In that scenario, ED would not be > MD -- Maybe: OR( SD <= MD and ED = SD)
           ) -- Excluding = ED prevents MRR from being double counted on Renewal
        OR  -- Issue is when this date is in the future as the renewal has not happened yet. Normally this is controlled for by allowing
           (
           -- Start Date < movement date (only allow if )
               movement_date                      >  CURRENT_DATE()
           AND array_subset.adjustment_end_date  >  CURRENT_DATE()
           AND array_subset.adjustment_end_date  >= movement_date
           )
          ) -- End Date is later than all Start Dates. Then don't need to worry about it overlapping with the Start Date Renewal.
            -- If this date is in the future, then don't know that it isn't going to renew, so can count MRR.
;

alter table d_service_dates_adjustments add column asof_date datetime;
update d_service_dates_adjustments set asof_date=current_timestamp;


---------------------------------------------------
---- checkpoint revenue sum
---------------------------------------------------

create or replace temporary table
checkpoint_revenue_sum AS
SELECT
    adjustment_product_category
    ,adjustment_product_family
    ,ams_account_id
    ,account_name
    ,earliest_start
    ,final
    ,movement_date
    ,SUM(NVL(subtotal_amount, 0)) revenue_sum
    ,SUM(NVL(mrr, 0))         mrr_sum
FROM
    d_service_dates_adjustments
GROUP BY
    adjustment_product_category
    ,adjustment_product_family
    ,ams_account_id
    ,account_name
    ,movement_date
    ,earliest_start
    ,final
ORDER BY
    adjustment_product_category
    ,adjustment_product_family
    ,ams_account_id
    ,movement_date
;


-----------------------------------------
---- revenue movements
----------------------------------------


create or replace table revenue_movements_fact AS
WITH
lag_revenue_calculation AS (
SELECT
    *
    ,DATE_TRUNC('month', movement_date) AS movement_month
    ,LAG(mrr_sum, 1, 0) OVER (PARTITION BY adjustment_product_category, ams_account_id ORDER BY adjustment_product_category, ams_account_id, movement_date ASC) AS mrr_lag
    ,mrr_sum - mrr_lag AS mrr_delta
    ,CASE
        WHEN movement_date = earliest_start THEN 'New Business'
        WHEN mrr_delta > 0              THEN 'Expansion'
        WHEN mrr_delta < 0              THEN 'Contraction'
        WHEN mrr_delta < 0              THEN 'Contraction'
        ELSE                                     'Unknown'
        END AS movement_type
FROM
    checkpoint_revenue_sum
)

SELECT
    *
    ,CASE
        WHEN mrr_delta > 0              THEN 'Positive'
        WHEN mrr_delta < 0              THEN 'Negative'
        WHEN mrr_delta = 0              THEN 'Neutral'
        ELSE                                 'Unknown'
        END AS movement_direction
    ,CASE
        WHEN mrr_sum = 0                THEN 'Churned'
        WHEN mrr_sum > 0                THEN 'Active'
        ELSE                                 'Unknown'
        END AS movement_result
    ,CASE
        WHEN movement_type = 'New Business'                                            THEN 'New Business'
        WHEN (mrr_sum > 0 AND mrr_lag = 0 AND movement_type != 'New Business')         THEN 'Reactivation'
        WHEN  mrr_sum = 0                                                              THEN 'Churned'
        ELSE                                                                                'Unknown'
        END AS movement_relation
    ,LAG(movement_date, 1, NULL) OVER (PARTITION BY adjustment_product_category, ams_account_id ORDER BY adjustment_product_category, ams_account_id, movement_date ASC) AS movement_date_lag
    ,CASE
        WHEN DATEDIFF('month', movement_date_lag, movement_date) >= 3 THEN '3 month gap'
        ELSE                                                               'Short gap'
        END AS movement_gap
    ,CASE
        WHEN (movement_relation = 'Reactivation' AND DATEDIFF('month', movement_date_lag, movement_date) >=3) THEN 'New Business'
        WHEN (movement_relation = 'Reactivation' AND DATEDIFF('month', movement_date_lag, movement_date) < 3) THEN 'Expansion'
        ELSE                                                                                                       'Unknown'
        END AS reactivation_type
    ,CASE
        WHEN (movement_relation = 'Reactivation' AND DATEDIFF('month', movement_date_lag, movement_date) >=3) THEN 'New Business - Reactivation'
        WHEN (movement_relation = 'Reactivation' AND DATEDIFF('month', movement_date_lag, movement_date) < 3) THEN 'Expansion - Reactivation'
        WHEN  movement_type = 'New Business'                                                                  THEN 'New Business'
        WHEN mrr_delta > 0                                                                                    THEN 'Expansion'
        WHEN (mrr_sum = 0)                                                                                    THEN 'Churned'
        WHEN (mrr_sum > 0 AND mrr_delta < 0)                                                                  THEN 'Contraction'
        ELSE                                                                                                       'Unknown'
        END AS movement_type_final
FROM
    lag_revenue_calculation
;

alter table revenue_movements_fact add column asof_date datetime;
update revenue_movements_fact set asof_date=current_timestamp;

  -----------------------------------------
  ------ Monthly Budget
  -----------------------------------------

  create or replace table ads_budget_monthly_fact
  as
  WITH budget as (select
        distinct b.ACCOUNT__C as sfdc_account,
        replace(m.PARENT_BID__C, ' ', '') as parent_id,
        nvl(h.PARENT_NAME,b.ACCOUNT__C) as parent_name,
        c.name,
        b.BUDGET_START_DATE__C::date as BUDGET_START_DATE,
        b.BUDGET_END_DATE__C::date as BUDGET_END_DATE,
        BUDGET_PERIOD_DAYS__C,
        CASE WHEN date_trunc(month,BUDGET_START_DATE) = date_trunc(month,current_date) -- budget period in the current month
                  then DATEDIFF(day,BUDGET_START_DATE, LEAST(BUDGET_END_DATE,current_date)) + 1
             WHEN date_trunc(month,BUDGET_START_DATE) > date_trunc(month,current_date) then 0
                  else BUDGET_PERIOD_DAYS__C end as budget_days, -- for current month, calculate # of days passed as budget period
        b.BUDGET__C,
        NVL(b.BUDGET__C,0)/NVL(BUDGET_PERIOD_DAYS__C,1) as daily_budget
        from ZENALYTICS.SFDC.BUDGET__C b
        left join ZENALYTICS.SFDC.ADVERTISING_CAMPAIGN__C c
        on b.ADVERTISING_CAMPAIGN__C = c.id
        left join ZENALYTICS.SFDC.ACCOUNT_MANAGEMENT__C m
        on m.id = c.AMO__C
        left join ZENALYTICS.CRM.BUSINESSPROFILE_HIERARCHY h
        on h.parent_id = m.PARENT_BID__C
        where b.ACCOUNT__C != 'ZENREACH OPERATIONS'
        and b.ISDELETED = false
        -- exclude test campaigns
        and nvl(c.test_campaign__c, false) != true
        and c.CAMPAIGN_STATUS__C != 'Onboarding'
        )
    select  sfdc_account,
            NVL(parent_id,'Unknown') as parent_id,
            parent_name,
            date_trunc(month,budget_start_date) as budget_month,
            sum(daily_budget*budget_days) as prorated_budget,
            sum(budget__c) as budget
            from budget b
            group by 1,2,3,4
            order by 1,4;

  alter table ads_budget_monthly_fact add column asof_date datetime;
  update ads_budget_monthly_fact set asof_date=current_timestamp;

  -----------------------------------------
  ------ Ad spend
  -----------------------------------------

  create or replace table zenalytics.revrec.ads_spend_monthly_fact
  as
  WITH
      month_list as (select report_date as report_month from zenalytics.revrec.d_date where day_of_mon = 1 and year >= 2020 and year <= 2023),
      parent_list as (select distinct parent_id, parent_name from ZENALYTICS.ADS.CAMPAIGN_INSIGHT_METRICS where INSIGHT_TYPE = 'AGGREGATE'),
      month_ts as (select parent_id, parent_name, report_month
                   from month_list, parent_list),
      spend_ts as (
                  select  m.PARENT_ID,
                          m.PARENT_NAME,
                          date_trunc('month',date) as spend_month, -- monthly spend
                          count(distinct case when channel = 'FB' then campaign_id else null end) as num_fb_campaigns,
                          count(distinct case when channel = 'LR' then campaign_id else null end) as num_lr_campaigns,
                          sum(case when channel = 'FB' then PLATFORM_SPEND else 0 end) as platform_spend_fb,
                          sum(case when channel = 'LR' then PLATFORM_SPEND else 0 end) as platform_spend_lr,
                          sum(case when channel = 'FB' then PLATFORM_SPEND/nvl((1-margin), 1) else 0 end) as client_spend_fb,
                          sum(case when channel = 'LR' then PLATFORM_SPEND/nvl((1-margin), 1) else 0 end) as client_spend_lr,
                          sum(PLATFORM_SPEND) as platform_spend,
                          sum(PLATFORM_SPEND/nvl((1-margin), 1)) as client_spend -- client spend
                          from ZENALYTICS.ADS.CAMPAIGN_INSIGHT_METRICS m
                          where INSIGHT_TYPE = 'AGGREGATE'
                          group by 1,2,3
                  )
  select  m.parent_id,
          m.parent_name,
          m.report_month as spend_month,
          NVL(num_fb_campaigns,0) as num_fb_campaigns,
          NVL(num_lr_campaigns,0) as num_lr_campaigns,
          NVL(platform_spend_fb,0) as platform_spend_fb,
          NVL(platform_spend_lr,0) as platform_spend_lr,
          NVL(client_spend_fb,0)   as client_spend_fb,
          NVL(client_spend_lr,0)   as client_spend_lr,
          NVL(s.platform_spend,0) as platform_spend,
          NVL(s.client_spend,0) as client_spend
          from month_ts m left join spend_ts s on m.parent_id = s.parent_id and m.report_month = s.spend_month
         ;

alter table ads_spend_monthly_fact add column asof_date datetime;
update ads_spend_monthly_fact set asof_date=current_timestamp;
