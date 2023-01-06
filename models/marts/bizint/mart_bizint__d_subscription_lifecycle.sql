WITH FUTURE_DATE AS (
    SELECT last_day(dateadd(YEAR, 10, current_date()), YEAR) AS FD, to_date('1970-01-01') AS PD
),
     DELINQUENCIES AS (
         SELECT RECURLY_SUBSCRIPTION_ID,
                min(SUBSCRIPTION_FIRST_REALIZED_DATE) AS SUBSCRIPTION_FIRST_REALIZED_DATE,
                min(DELINQUENCY_BEGIN_DATE)           AS DELINQUENCY_BEGIN_DATE,
                max(CASE
                        WHEN DELINQUENCY_END_DATE <> FUTURE_DATE.FD THEN DELINQUENCY_END_DATE
                        ELSE NULL END)                AS DELINQUENCY_END_DATE
         FROM {{ ref('mart_bizint__d_delinquency_lifecycle') }},
              FUTURE_DATE
         GROUP BY RECURLY_SUBSCRIPTION_ID
     ),
     POST_MIGRATION AS (
         SELECT DISTINCT V3_ACCOUNT_ID,
                         RECURLY_SUBSCRIPTION_ID,
                         MIGRATED_DATE,
                         SUBSCRIPTION_REACTIVATED_DATE
         FROM {{ ref('stg_ams__subscriptions_v2_post_migration') }}
     ),
     _D_SUBSCRIPTION_LIFECYCLE AS (
         SELECT SUBSCRIPTION_CREATE_DATE,
                SUBSCRIPTION_UPDATE_DATE,
                STATE_BEGIN_DATE,
                STATE_END_DATE,
                TS.ACCOUNT_ID,
                TS.SUBSCRIPTION_ID,
                TS.RECURLY_SUBSCRIPTION_ID,
                SUBSCRIPTION_SK,
                PRODUCT,
                PACKAGE,
                PLAN_CODE,
                MANUAL_INVOICE_IND,
                TS.SUBSCRIPTION_START_DATE,
                MONTHLY_SUBSCRIPTION_SERVICE_FEE,
                SUBSCRIPTION_ACTIVE_IND,
                SUBSCRIPTION_CANCELLED_DATE,
                LICENSE_QUANTITY,
                NVL(MIGRATED_DATE, FUTURE_DATE.PD)                                                              AS SUBSCRIPTION_MIGRATED_DATE,    -- if not migrated account, swt date in the past
                CASE
                    WHEN M.V3_ACCOUNT_ID IS NOT NULL THEN NVL(SUBSCRIPTION_FIRST_REALIZED_DATE, SUBSCRIPTION_START_DATE)
                    ELSE NVL(SUBSCRIPTION_FIRST_REALIZED_DATE, FUTURE_DATE.FD) END                              AS SUBSCRIPTION_FIRST_REALIZED_DATE,
--        NVL(subscription_first_realized_date, future_date.fd) as subscription_first_realized_date,
                NVL(DELINQUENCY_BEGIN_DATE, FUTURE_DATE.FD)                                                     AS DELINQUENCY_BEGIN_DATE,
                NVL(DELINQUENCY_END_DATE, FUTURE_DATE.FD)                                                       AS DELINQUENCY_END_DATE,
                RANK()
                        OVER (PARTITION BY TS.SUBSCRIPTION_ID ORDER BY SUBSCRIPTION_UPDATE_DATE ASC)            AS SUBSCRIPTION_LIFECYCLE_NUMBER, -- each subs lifecycle, each subs can have multiple lifecycles, all of same subscription id
                DENSE_RANK()
                        OVER (PARTITION BY TS.ACCOUNT_ID, TS.SUBSCRIPTION_ID ORDER BY SUBSCRIPTION_UPDATE_DATE) AS SUBSCRIPTION_SEQ_NUMBER,       -- sequence of all subscription records within each account
                LAG(MONTHLY_SUBSCRIPTION_SERVICE_FEE)
                    OVER (PARTITION BY TS.SUBSCRIPTION_ID ORDER BY SUBSCRIPTION_UPDATE_DATE ASC)                AS PREV_MONTHLY_SUBSCRIPTION_SERVICE_FEE,
                LEAD(MONTHLY_SUBSCRIPTION_SERVICE_FEE)
                     OVER (PARTITION BY TS.SUBSCRIPTION_ID ORDER BY SUBSCRIPTION_UPDATE_DATE ASC)               AS NEXT_MONTHLY_SUBSCRIPTION_SERVICE_FEE,
                NVL(LAG(LICENSE_QUANTITY) OVER (PARTITION BY TS.SUBSCRIPTION_ID ORDER BY SUBSCRIPTION_UPDATE_DATE ASC),
                    0)                                                                                          AS PREV_LICENSE_QUANTITY,
                NVL(LEAD(LICENSE_QUANTITY) OVER (PARTITION BY TS.SUBSCRIPTION_ID ORDER BY SUBSCRIPTION_UPDATE_DATE ASC),
                    0)                                                                                          AS NEXT_LICENSE_QUANTITY,
                NVL(LAG(SUBSCRIPTION_CANCELLED_DATE)
                        OVER (PARTITION BY TS.ACCOUNT_ID ORDER BY TS.SUBSCRIPTION_START_DATE ASC),
                    FUTURE_DATE.FD)                                                                             AS PREV_CANCELLATION_DATE,
                NVL(LEAD(SUBSCRIPTION_FIRST_REALIZED_DATE)
                         OVER (PARTITION BY TS.ACCOUNT_ID ORDER BY SUBSCRIPTION_FIRST_REALIZED_DATE, SUBSCRIPTION_CREATE_DATE ASC),
                    FUTURE_DATE.FD)                                                                             AS NEXT_SUBSCRIPTION_REALIZED_DATE,
                NVL(LEAD(TS.SUBSCRIPTION_START_DATE)
                         OVER (PARTITION BY TS.ACCOUNT_ID ORDER BY TS.SUBSCRIPTION_START_DATE ASC),
                    FUTURE_DATE.FD)                                                                             AS NEXT_SUBSCRIPTION_START_DATE,
                CASE
                    WHEN M.V3_ACCOUNT_ID IS NOT NULL AND TS.SUBSCRIPTION_START_DATE > M.MIGRATED_DATE
                        THEN TS.SUBSCRIPTION_START_DATE
                    ELSE M.SUBSCRIPTION_REACTIVATED_DATE END                                                    AS MNIR_DATE,                     -- add reactivation logic 11/7 for migrated accounts: MNIR = migrated_no_invoice_realized
                CASE
                    WHEN M.V3_ACCOUNT_ID IS NOT NULL AND TS.SUBSCRIPTION_START_DATE > M.MIGRATED_DATE THEN TRUE
                    ELSE FALSE END                                                                              AS CNM_FL
         FROM {{ ref('mart_bizint__subscription_ts') }} TS
                  LEFT JOIN DELINQUENCIES D
                            ON TS.RECURLY_SUBSCRIPTION_ID = D.RECURLY_SUBSCRIPTION_ID
                  LEFT JOIN POST_MIGRATION M
                            ON M.V3_ACCOUNT_ID = TS.ACCOUNT_ID AND
                               M.RECURLY_SUBSCRIPTION_ID = TS.RECURLY_SUBSCRIPTION_ID -- added reactivation date logic 11/7
                  INNER JOIN FUTURE_DATE
     ),
     OFFSETTING_CHURN AS (
         SELECT DISTINCT SUBSCRIPTION_ID,
                         SUBSCRIPTION_CANCELLED_DATE
         FROM {{ ref('mart_bizint__d_offsetting_subscriptions') }} S),
     OFFSETTING_MVMTS AS (
         SELECT DISTINCT OFFSETTING_SUBSCRIPTION_ID,
                         OFFSETTING_SUBSCRIPTION_CREATE_DATE
         FROM {{ ref('mart_bizint__d_offsetting_subscriptions') }} S)
SELECT SUBSCRIPTION_CREATE_DATE,
       SUBSCRIPTION_UPDATE_DATE,
       ACCOUNT_ID,
       D.SUBSCRIPTION_ID,
       RECURLY_SUBSCRIPTION_ID,
       trim('v3' || ':' || D.SUBSCRIPTION_ID)                                          AS SUBSCRIPTION_SK,
       SUBSCRIPTION_LIFECYCLE_NUMBER,
       SUBSCRIPTION_SEQ_NUMBER,
       PRODUCT,
       PACKAGE,
       PLAN_CODE,
       MANUAL_INVOICE_IND,
       SUBSCRIPTION_START_DATE,
       SUBSCRIPTION_MIGRATED_DATE,                                                                                   -- new field
       MONTHLY_SUBSCRIPTION_SERVICE_FEE,
       SUBSCRIPTION_ACTIVE_IND,
       D.SUBSCRIPTION_CANCELLED_DATE,
       LICENSE_QUANTITY,
       D.SUBSCRIPTION_FIRST_REALIZED_DATE,
       DELINQUENCY_BEGIN_DATE,
       DELINQUENCY_END_DATE,
       CASE
           WHEN SUBSCRIPTION_LIFECYCLE_NUMBER = 1 THEN LEAST(SUBSCRIPTION_START_DATE, STATE_BEGIN_DATE)
           ELSE STATE_BEGIN_DATE END                                                   AS STATE_BEGIN_DATE,          -- beginning of a subscription state
       STATE_END_DATE,                                                                                               -- each subs can have multiple states
       CASE
           WHEN NVL(D.SUBSCRIPTION_FIRST_REALIZED_DATE, FUTURE_DATE.FD) != FUTURE_DATE.FD THEN TRUE
           ELSE FALSE END::BOOLEAN                                                     AS REALIZED_FL,
       CASE WHEN SUBSCRIPTION_START_DATE IS NOT NULL THEN TRUE ELSE FALSE END::BOOLEAN AS BOOKED_FL,
       CASE
           WHEN SUBSCRIPTION_LIFECYCLE_NUMBER > 1 AND
                MONTHLY_SUBSCRIPTION_SERVICE_FEE > PREV_MONTHLY_SUBSCRIPTION_SERVICE_FEE THEN STATE_BEGIN_DATE
           ELSE FUTURE_DATE.FD END                                                     AS SUBSCRIPTION_UPGRADE_DATE, -- upgrade = expansion
       CASE
           WHEN SUBSCRIPTION_LIFECYCLE_NUMBER > 1 AND
                MONTHLY_SUBSCRIPTION_SERVICE_FEE < PREV_MONTHLY_SUBSCRIPTION_SERVICE_FEE THEN STATE_BEGIN_DATE
           ELSE FUTURE_DATE.FD END                                                     AS SUBSCRIPTION_DOWNGRADE_DATE,
       LEAST(DELINQUENCY_BEGIN_DATE, D.SUBSCRIPTION_CANCELLED_DATE)                    AS CHURN_BEGIN_DATE,
       LEAST(DELINQUENCY_END_DATE, NEXT_SUBSCRIPTION_REALIZED_DATE)                    AS CHURN_END_DATE,
       CASE
           WHEN DELINQUENCY_BEGIN_DATE < D.SUBSCRIPTION_CANCELLED_DATE THEN DELINQUENCY_BEGIN_DATE
           ELSE FUTURE_DATE.FD END                                                     AS DELINQUENCY_CHURN_DATE,
       CASE
           WHEN DELINQUENCY_BEGIN_DATE > D.SUBSCRIPTION_CANCELLED_DATE THEN D.SUBSCRIPTION_CANCELLED_DATE
           ELSE FUTURE_DATE.FD END                                                     AS SUBSCRIPTION_CHURN_DATE,
       CASE
           WHEN OC.SUBSCRIPTION_ID IS NOT NULL AND D.SUBSCRIPTION_ACTIVE_IND = FALSE THEN D.SUBSCRIPTION_CANCELLED_DATE
           WHEN OM.OFFSETTING_SUBSCRIPTION_ID IS NOT NULL THEN OM.OFFSETTING_SUBSCRIPTION_CREATE_DATE
           ELSE FUTURE_DATE.FD END                                                     AS SUBSCRIPTION_ADJUSTMENT_DATE,
       MNIR_DATE,                                                                                                    -- added reactivation date 11/7
       CNM_FL,
       current_date                                                                    AS ASOF_DATE
FROM _D_SUBSCRIPTION_LIFECYCLE D
         LEFT JOIN OFFSETTING_CHURN OC
                   ON D.SUBSCRIPTION_ID = OC.SUBSCRIPTION_ID
         LEFT JOIN OFFSETTING_MVMTS OM
                   ON D.SUBSCRIPTION_ID = OM.OFFSETTING_SUBSCRIPTION_ID
         INNER JOIN FUTURE_DATE