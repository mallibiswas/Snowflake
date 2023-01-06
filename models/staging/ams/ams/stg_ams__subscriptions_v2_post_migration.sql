WITH MIGRATED_ACCTS AS (
    SELECT ACCOUNT_ID,
           V3_ACCOUNT_ID,
           to_date(NVL(MIGRATED_DATE, to_date('2029-12-31'))) AS MIGRATED_DATE
    FROM {{ ref('stg_ams__account') }}
    WHERE ACCOUNT_STATE <> 'CLOSED'
      AND IS_TEST = FALSE
),
     BEST_SNAPSHOT AS (
         SELECT A.ACCOUNT_ID,
                A.V3_ACCOUNT_ID,
                A.MIGRATED_DATE,
                SUBSCRIPTION_ID,
                min(S.ASOF_DATE) AS SNAPSHOT_DATE
         FROM MIGRATED_ACCTS A
                  LEFT JOIN {{ ref('stg_ams__subscriptions_v2_snapshot') }} S ON S.ACCOUNT_ID = A.ACCOUNT_ID
         WHERE S.ASOF_DATE > NVL(A.MIGRATED_DATE, current_date())
         GROUP BY 1, 2, 3, 4
         ORDER BY 1
     )
SELECT S.*,
       B.V3_ACCOUNT_ID,
       B.MIGRATED_DATE,
       to_date(RS.ACTIVATED_AT) AS SUBSCRIPTION_REACTIVATED_DATE
FROM {{ ref('stg_ams__subscriptions_v2_snapshot') }} S,
     BEST_SNAPSHOT B,
     {{ ref('stg_recurly__subscriptions') }} RS
WHERE S.SUBSCRIPTION_ID = B.SUBSCRIPTION_ID
  AND S.RECURLY_SUBSCRIPTION_ID = RS.RECURLY_SUBSCRIPTION_ID
  AND S.ASOF_DATE = B.SNAPSHOT_DATE
  AND S.SUBSCRIPTION_STATE != 'CANCELLED'

