WITH FUTURE_DATE AS (
    SELECT last_day(dateadd(YEAR, 10, current_date()), YEAR) AS FD
),
     ALLINVOICES AS (
         SELECT ACCOUNT_CODE,
                SUBSCRIPTION_ID,
                nvl(COLLECTION_METHOD, 'automatic')           AS COLLECTION_METHOD,
                STATE,
                CREATED_AT,
                dateadd('day', nvl(NET_TERMS, 0), CREATED_AT) AS DUE_DATE,
                INVOICE_NUMBER
         FROM {{ ref('stg_recurly__invoices') }}
         WHERE NET_TERMS IS NOT NULL
     )
        ,
     REFLIST AS (
         SELECT ACCOUNT_CODE,
                SUBSCRIPTION_ID,
                COLLECTION_METHOD,
                count(INVOICE_NUMBER) OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD)                                                                    AS NUMBER_OF_INVOICES,
                STATE,
                DUE_DATE,
                FIRST_VALUE(CREATED_AT)
                            OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD ORDER BY CREATED_AT)                                                          AS FIRST_INVOICED_AT,
                CASE
                    WHEN STATE IN ('collected') THEN FIRST_VALUE(CREATED_AT)
                                                                 OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD, STATE ORDER BY CREATED_AT) END          AS FIRST_PAID_AT,
                CASE
                    WHEN STATE IN ('collected') THEN LAST_VALUE(CREATED_AT)
                                                                OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD, STATE ORDER BY CREATED_AT) END           AS LAST_PAID_AT,
                CASE
                    WHEN STATE IN ('failed', 'past_due') THEN FIRST_VALUE(CREATED_AT)
                                                                          OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD, STATE ORDER BY CREATED_AT) END AS FIRST_UNPAID_AT,
                CASE
                    WHEN STATE IN ('failed', 'past_due') THEN LAST_VALUE(CREATED_AT)
                                                                         OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD, STATE ORDER BY CREATED_AT) END  AS LAST_UNPAID_AT,
                CASE
                    WHEN STATE <> lag(STATE) OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD ORDER BY CREATED_AT)
                        THEN CREATED_AT
                    WHEN INVOICE_NUMBER = FIRST_VALUE(INVOICE_NUMBER)
                                                      OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD ORDER BY CREATED_AT)
                        THEN CREATED_AT
                    WHEN INVOICE_NUMBER = LAST_VALUE(INVOICE_NUMBER)
                                                     OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD ORDER BY CREATED_AT)
                        THEN CREATED_AT
                    ELSE NULL END                                                                                                                                 AS REF_CREATED_AT,
                CASE
                    WHEN STATE <> lag(STATE) OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD ORDER BY CREATED_AT)
                        THEN INVOICE_NUMBER
                    WHEN INVOICE_NUMBER = FIRST_VALUE(INVOICE_NUMBER)
                                                      OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD ORDER BY CREATED_AT)
                        THEN INVOICE_NUMBER
                    WHEN INVOICE_NUMBER = LAST_VALUE(INVOICE_NUMBER)
                                                     OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD ORDER BY CREATED_AT)
                        THEN INVOICE_NUMBER
                    ELSE NULL END                                                                                                                                 AS REF_INVOICE_NUMBER
         FROM ALLINVOICES
     ),
     STATE_BOUNDARIES AS (
         SELECT ACCOUNT_CODE,
                SUBSCRIPTION_ID,
                COLLECTION_METHOD,
                STATE,
                NUMBER_OF_INVOICES,
                DUE_DATE,
                FIRST_INVOICED_AT,
                FIRST_PAID_AT,
                LAST_PAID_AT,
                FIRST_UNPAID_AT,
                LAST_UNPAID_AT,
                B.REF_CREATED_AT                                                                                                       AS STATE_BEGIN_AT,
                lead(REF_CREATED_AT)
                     OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD ORDER BY REF_CREATED_AT)                                  AS STATE_END_AT,
                datediff(DAY, B.REF_CREATED_AT, lead(REF_CREATED_AT)
                                                     OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD ORDER BY REF_CREATED_AT)) AS DAYS_IN_STATE, -- = days betwn state begin and state end
                B.REF_INVOICE_NUMBER                                                                                                   AS BEGIN_INVOICE_NUMBER,
                lead(REF_INVOICE_NUMBER)
                     OVER (PARTITION BY SUBSCRIPTION_ID, COLLECTION_METHOD ORDER BY REF_INVOICE_NUMBER)                              AS END_INVOICE_NUMBER
         FROM REFLIST B
         WHERE REF_INVOICE_NUMBER IS NOT NULL
     )
SELECT S.ACCOUNT_CODE                                                               AS RECURLY_ACCOUNT_ID,
       A.COMPANY_NAME                                                               AS ACCOUNT_NAME,
       S.SUBSCRIPTION_ID                                                            AS RECURLY_SUBSCRIPTION_ID,
       S.COLLECTION_METHOD,
       S.STATE,
       DUE_DATE,
       NUMBER_OF_INVOICES,
       STATE_BEGIN_AT                                                               AS STATE_BEGIN_DATE,
       STATE_END_AT                                                                 AS STATE_END_DATE,
       FIRST_INVOICED_AT                                                            AS FIRST_INVOICED_DATE,
       FIRST_PAID_AT                                                                AS INVOICE_FIRST_PAID_DATE,
       LAST_PAID_AT                                                                 AS INVOICE_LAST_PAID_DATE,
       FIRST_UNPAID_AT                                                              AS INVOICE_FIRST_UNPAID_DATE,
       LAST_UNPAID_AT                                                               AS INVOICE_LAST_UNPAID_DATE,
       CASE
           WHEN S.COLLECTION_METHOD = 'automatic' THEN FIRST_PAID_AT
           ELSE FIRST_INVOICED_AT END                                               AS SUBSCRIPTION_REALIZED_DATE,
       CASE
           WHEN S.COLLECTION_METHOD = 'automatic' THEN MIN(FIRST_PAID_AT)
                                                         OVER (PARTITION BY S.SUBSCRIPTION_ID ORDER BY DUE_DATE ASC)
           ELSE MIN(FIRST_INVOICED_AT)
                    OVER (PARTITION BY S.SUBSCRIPTION_ID ORDER BY DUE_DATE ASC) END AS SUBSCRIPTION_FIRST_REALIZED_DATE,
       DAYS_IN_STATE,
       BEGIN_INVOICE_NUMBER,
       END_INVOICE_NUMBER,
       CASE
           WHEN S.STATE IN ('failed', 'past_due') AND DAYS_IN_STATE > 120 THEN dateadd(DAY, 120, STATE_BEGIN_AT)
           ELSE FUTURE_DATE.FD END                                                  AS DELINQUENCY_BEGIN_DATE,
       CASE
           WHEN S.STATE IN ('failed', 'past_due') AND DAYS_IN_STATE > 120 THEN STATE_END_AT
           ELSE FUTURE_DATE.FD END                                                  AS DELINQUENCY_END_DATE,
       rank() OVER (PARTITION BY S.SUBSCRIPTION_ID ORDER BY STATE_BEGIN_AT ASC)     AS DELINQUENCY_LIFECYCLE_NUMBER,
       current_date                                                                 AS ASOF_DATE
FROM STATE_BOUNDARIES S,
     {{ ref('stg_recurly__invoices') }} A,
     FUTURE_DATE
WHERE S.ACCOUNT_CODE = A.ACCOUNT_CODE
  AND (CASE WHEN NUMBER_OF_INVOICES = 1 THEN 1 ELSE END_INVOICE_NUMBER END) IS NOT NULL