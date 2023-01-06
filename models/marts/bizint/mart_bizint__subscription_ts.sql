WITH
-- demo or nfr accounts have no paid subscription at all
DEMO_NFR_ACCTS AS (SELECT ACCOUNT_ID
                   FROM {{ ref('stg_ams_account__subscription') }} S,
                        {{ ref('stg_ams_account__recurly_subscription') }} RS
                   WHERE S.ID = RS.RECURLY_SUBSCRIPTION_ID
                   GROUP BY 1
                   HAVING sum(UNIT_PRICE_CENTS) = 0),
FUTURE_DATE AS (SELECT last_day(dateadd(YEAR, 10, current_date()), YEAR) AS FD),
SUBSCRIPTION_STATES AS (
    SELECT S.CREATED AS STATE_BEGIN_DATE,
           SS.CREATED AS STATE_END_DATE,
           SS.ACCOUNT_ID,
           SS.ID AS SUBSCRIPTION_ID,
           trim('v3' || ':' || SS.ID) AS SUBSCRIPTION_SK,
           RSS.RECURLY_SUBSCRIPTION_ID,
           SS.PRODUCT,
           SS.PACKAGE,
           RSS.PLAN_CODE,
           SS.MANUAL_INVOICE AS MANUAL_INVOICE_IND,
           RSS.START_DATE AS SUBSCRIPTION_START_DATE,
           trunc(
                   RSS.UNIT_PRICE_CENTS * RSS.QUANTITY / RSS.BILLING_FREQUENCY_MONTHS) AS MONTHLY_SUBSCRIPTION_SERVICE_FEE,
           RSS.ACTIVE AS SUBSCRIPTION_ACTIVE_IND, -- convert boolean to 0/1 to assist summarization
           CASE WHEN RSS.ACTIVE = FALSE THEN RSS.CREATED ELSE FUTURE_DATE.FD END AS SUBSCRIPTION_CANCELLED_DATE,
           RSS.QUANTITY AS LICENSE_QUANTITY
    FROM {{ ref('stg_ams_account__subscription') }} S,
         {{ ref('stg_ams_account__subscription_snapshot') }} SS,
         {{ ref('stg_ams_account__recurly_subscription_snapshot') }} RSS,
         FUTURE_DATE
    WHERE SS.RECURLY_SUBSCRIPTION_SNAPSHOT_ID = RSS.ID
      AND S.ID = SS.SUBSCRIPTION_ID
    UNION ALL
    SELECT NVL(SS.UPDATED, SS.CREATED) AS STATE_BEGIN_DATE,
           FUTURE_DATE.FD AS STATE_END_DATE,
           SS.ACCOUNT_ID,
           SS.ID AS SUBSCRIPTION_ID,
           trim('v3' || ':' || SS.ID) AS SUBSCRIPTION_SK,
           RSS.RECURLY_SUBSCRIPTION_ID,
           SS.PRODUCT,
           SS.PACKAGE,
           RSS.PLAN_CODE,
           SS.MANUAL_INVOICE AS MANUAL_INVOICE_IND,
           RSS.START_DATE AS SUBSCRIPTION_START_DATE,
           trunc(
                   RSS.UNIT_PRICE_CENTS * RSS.QUANTITY / RSS.BILLING_FREQUENCY_MONTHS) AS MONTHLY_SUBSCRIPTION_SERVICE_FEE,
           RSS.ACTIVE AS SUBSCRIPTION_ACTIVE_IND, -- convert boolean to 0/1 to assist summarization
           CASE WHEN RSS.ACTIVE = FALSE THEN RSS.UPDATED ELSE FUTURE_DATE.FD END AS SUBSCRIPTION_CANCELLED_DATE,
           RSS.QUANTITY AS LICENSE_QUANTITY
    FROM {{ ref('stg_ams_account__subscription') }} SS,
         {{ ref('stg_ams_account__recurly_subscription') }} RSS,
         FUTURE_DATE
    WHERE SS.RECURLY_SUBSCRIPTION_ID = RSS.RECURLY_SUBSCRIPTION_ID
)
SELECT -- LEAST(subscription_start_date,FIRST_VALUE(state_begin_date) over (partition by account_id, subscription_id order by state_end_date)) as subsciption_create_date, -- Migrated accounts can have subs created after started
       FIRST_VALUE(STATE_BEGIN_DATE)
                   OVER (PARTITION BY ACCOUNT_ID, SUBSCRIPTION_ID ORDER BY STATE_END_DATE) AS SUBSCRIPTION_CREATE_DATE,
       NVL(LAG(STATE_END_DATE) OVER (PARTITION BY ACCOUNT_ID, SUBSCRIPTION_ID ORDER BY STATE_END_DATE),
           SUBSCRIPTION_START_DATE)                                                        AS STATE_BEGIN_DATE,
       STATE_END_DATE,
       STATE_END_DATE                                                                      AS SUBSCRIPTION_UPDATE_DATE,
       ACCOUNT_ID,
       SUBSCRIPTION_ID,
       SUBSCRIPTION_SK,
       RECURLY_SUBSCRIPTION_ID,
       PRODUCT,
       PACKAGE,
       PLAN_CODE,
       MANUAL_INVOICE_IND,
       SUBSCRIPTION_START_DATE,
       MONTHLY_SUBSCRIPTION_SERVICE_FEE,
       SUBSCRIPTION_ACTIVE_IND,
       SUBSCRIPTION_CANCELLED_DATE,
       LICENSE_QUANTITY
FROM SUBSCRIPTION_STATES
WHERE ACCOUNT_ID NOT IN (SELECT ACCOUNT_ID FROM DEMO_NFR_ACCTS) -- exclude demo/nfr accounts