WITH CHURN_REQUESTS AS (
    SELECT ACCOUNT__C                                                                                   AS SALESFORCE_ACCOUNT_ID,
           CHURN_SUMMARY__C                                                                             AS CHURN_SUMMARY,
           CHURN_STATUS__C                                                                              AS CHURN_STATUS,
           CONCAT(
                   IFF(CHURN_REASON_BUSINESS_SITUATION__C IS NULL, '',
                       CONCAT(CHURN_REASON_BUSINESS_SITUATION__C, ';', CHR(13), CHR(10))),
                   IFF(CHURN_REASON_CONTRACT_BILLING__C IS NULL, '',
                       CONCAT(CHURN_REASON_CONTRACT_BILLING__C, ';', CHR(13), CHR(10))),
                   IFF(CHURN_REASON_DETAILS__C IS NULL, '', CONCAT(CHURN_REASON_DETAILS__C, ';', CHR(13), CHR(10))),
                   IFF(CHURN_REASON_PRODUCT__C IS NULL, '', CONCAT(CHURN_REASON_PRODUCT__C, ';', CHR(13), CHR(10))),
                   IFF(CHURN_REASON_SERVICE__C IS NULL, '', CONCAT(CHURN_REASON_SERVICE__C, ';', CHR(13), CHR(10))),
                   IFF(CHURN_REASON_TECHNICAL__C IS NULL, '', CONCAT(CHURN_REASON_TECHNICAL__C, ';', CHR(13), CHR(10)))
               )                                                                                        AS CHURN_REASON,
           row_number() OVER (PARTITION BY ACCOUNT__C ORDER BY CREATEDDATE DESC, CHURN_SUMMARY__C DESC) AS RN
    FROM {{ ref('stg_sfdc__churn_request__c') }}
)
SELECT O.ACCOUNT_ID,
       A.SUBSCRIPTION_ID,
       SUBS.PRODUCT,
       CR.SALESFORCE_ACCOUNT_ID,
       SACCT.NAME,
       O.ID AS ORDER_ID,
       A.ID AS ASSET_ID,
       to_date(left(SO.EFFECTIVE_DATE, 10)) AS CHURN_REQUESTED_DATE,
       NOT (RSUBS.ACTIVE)         AS CANCELLED, -- cancelled subs
       CR.CHURN_STATUS,
       CR.CHURN_SUMMARY,
       CR.CHURN_REASON,
       RSUBS.RECURLY_SUBSCRIPTION_ID,
       RSUBS.UPDATED              AS CANCELLED_DATE,
       RSUBS.QUANTITY,
       RSUBS.UNIT_PRICE_CENTS,
       RSUBS.BILLING_FREQUENCY_MONTHS,
       current_date               AS ASOF_DATE
FROM {{ ref('stg_ams_account__orders' ) }} O,
    {{ ref('stg_ams_account__order_item') }} OI,
    {{ ref('stg_ams_account__asset') }} A,
    {{ ref('stg_ams_account__salesforce_order') }} SO,
    {{ ref('stg_ams_account__subscription') }} SUBS,
    {{ ref('stg_ams_account__recurly_subscription') }} RSUBS,
    {{ ref('stg_ams_account__account') }} ACCT,
    {{ ref('stg_ams_account__salesforce_account') }} SACCT,
    CHURN_REQUESTS CR
WHERE SUBS.RECURLY_SUBSCRIPTION_ID = RSUBS.RECURLY_SUBSCRIPTION_ID
  AND A.SUBSCRIPTION_ID = SUBS.ID
  AND O.ACCOUNT_ID = ACCT.ID
  AND ACCT.SALESFORCE_ACCOUNT = CR.SALESFORCE_ACCOUNT_ID
  AND SACCT.ID = CR.SALESFORCE_ACCOUNT_ID
  AND O.SALESFORCE_ORDER_ID = SO.ID
-- and to_date(left(so.type, 10)) = to_date(rsubs.updated) -- churn request date = cancelled date
  AND RSUBS.ACTIVE = FALSE
  AND O.ID = OI.ORDER_ID
  AND A.ID = OI.ASSET_ID
  AND O.CANCELLED IS NULL
  AND OI.SALESFORCE_ASSET_SYNCED = TRUE
  AND OI.DIRTY = FALSE
  AND to_date(A.CREATED) <> to_date(A.UPDATED)
  AND A.ITEM_TYPE = 'subscription'
  AND CR.RN = 1
ORDER BY CHURN_REQUESTED_DATE DESC
