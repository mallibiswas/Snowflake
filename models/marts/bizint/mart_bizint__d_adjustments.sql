/***
way to solve this problem
1. create anchor key on (account_id,subscription_create_date,product,rownum)
2. create a master list of all the anchor keys by select key from before union select key from after
3. left join before and after to the master list
*/

-- BEFORE / Subscription level
-- find subs that where cancelled with corresponding subs created at the same time
WITH ACTIVE_ACCTS AS (
    SELECT DISTINCT ACCOUNT_ID, PRODUCT, PLAN_CODE, SUBSCRIPTION_CANCELLED_DATE, SUBSCRIPTION_CREATE_DATE
    FROM {{ ref('mart_bizint__subscription_ts') }}
    WHERE SUBSCRIPTION_ACTIVE_IND = TRUE
),
     CANCELLED_ACCTS AS (
         SELECT DISTINCT ACCOUNT_ID, PRODUCT, PLAN_CODE, SUBSCRIPTION_CANCELLED_DATE, SUBSCRIPTION_CREATE_DATE
         FROM {{ ref('mart_bizint__subscription_ts') }}
         WHERE SUBSCRIPTION_ACTIVE_IND = FALSE
     ),
     _BEFORE_ AS (
         SELECT BEF.ACCOUNT_ID,
                SUBSCRIPTION_CREATE_DATE,
                SUBSCRIPTION_START_DATE,
                BEF.SUBSCRIPTION_ID,
                BEF.RECURLY_SUBSCRIPTION_ID,
                PRODUCT,
                PLAN_CODE,
                MONTHLY_SUBSCRIPTION_SERVICE_FEE,
                LICENSE_QUANTITY,
                SUBSCRIPTION_CANCELLED_DATE,
                row_number()
                        OVER (PARTITION BY BEF.ACCOUNT_ID, SUBSCRIPTION_CANCELLED_DATE ORDER BY SUBSCRIPTION_START_DATE) AS SEQNUM
         FROM {{ ref('mart_bizint__subscription_ts') }} BEF
         WHERE exists(SELECT *
                      FROM ACTIVE_ACCTS
                      WHERE ACTIVE_ACCTS.ACCOUNT_ID = BEF.ACCOUNT_ID
                        AND ACTIVE_ACCTS.SUBSCRIPTION_CREATE_DATE = BEF.SUBSCRIPTION_CANCELLED_DATE
                        AND PRODUCT = BEF.PRODUCT
                        AND PLAN_CODE = BEF.PLAN_CODE)
           AND SUBSCRIPTION_ACTIVE_IND = FALSE
     ),
     _AFTER_ AS (SELECT ACCOUNT_ID,
                        SUBSCRIPTION_CREATE_DATE,
                        SUBSCRIPTION_START_DATE,
                        SUBSCRIPTION_ID,
                        RECURLY_SUBSCRIPTION_ID,
                        PRODUCT,
                        PLAN_CODE,
                        MONTHLY_SUBSCRIPTION_SERVICE_FEE,
                        LICENSE_QUANTITY,
                        SUBSCRIPTION_CANCELLED_DATE,
                        row_number()
                                OVER (PARTITION BY ACCOUNT_ID,SUBSCRIPTION_CREATE_DATE ORDER BY SUBSCRIPTION_START_DATE) AS SEQNUM
                 FROM {{ ref('mart_bizint__subscription_ts') }} AFT
                 WHERE exists(SELECT 'x'
                              FROM CANCELLED_ACCTS
                              WHERE CANCELLED_ACCTS.ACCOUNT_ID = AFT.ACCOUNT_ID
                                AND CANCELLED_ACCTS.SUBSCRIPTION_CANCELLED_DATE = AFT.SUBSCRIPTION_CREATE_DATE
                                AND PRODUCT = AFT.PRODUCT
                                AND PLAN_CODE = AFT.PLAN_CODE)),


     _KEYS_ AS (
         SELECT ACCOUNT_ID,
                PRODUCT,
                PLAN_CODE,
                SUBSCRIPTION_CANCELLED_DATE AS EVENT_DATETIME,
                SEQNUM
         FROM _BEFORE_
         UNION
         SELECT ACCOUNT_ID,
                PRODUCT,
                PLAN_CODE,
                SUBSCRIPTION_CREATE_DATE AS EVENT_DATETIME,
                SEQNUM
         FROM _AFTER_
     )
SELECT K.ACCOUNT_ID,
       K.EVENT_DATETIME,
       K.PRODUCT,
       K.PLAN_CODE,
       K.SEQNUM,
       A.SALESFORCE_ACCOUNT,
       BEF.SUBSCRIPTION_ID,
       BEF.RECURLY_SUBSCRIPTION_ID,
       BEF.SUBSCRIPTION_START_DATE::DATE                                                                AS SUBSCRIPTION_START_DATE,
       BEF.MONTHLY_SUBSCRIPTION_SERVICE_FEE / 100                                                       AS MRR,
       BEF.LICENSE_QUANTITY                                                                             AS QUANTITY,
       BEF.SUBSCRIPTION_CANCELLED_DATE,
       AFT.SUBSCRIPTION_ID                                                                              AS SUBSCRIPTION_ID_AFTER_PAUSE,
       AFT.SUBSCRIPTION_CREATE_DATE,
       AFT.LICENSE_QUANTITY                                                                             AS QUANTITY_AFTER_PAUSE,
       ADD_MONTHS(AFT.SUBSCRIPTION_START_DATE, -1 * floor(datediff(MONTH, AFT.SUBSCRIPTION_CREATE_DATE,
                                                                   AFT.SUBSCRIPTION_START_DATE)))::DATE AS PAUSED_DATE,
       datediff(DAY, PAUSED_DATE, AFT.SUBSCRIPTION_START_DATE)                                          AS DAYS_PAUSED, -- diff btwn paused date and next start date
       AFT.SUBSCRIPTION_START_DATE::DATE                                                                AS START_DATE_AFTER_PAUSE,
       AFT.RECURLY_SUBSCRIPTION_ID                                                                      AS RECURLY_SUBSCRIPTION_ID_AFTER_PAUSE,
       AFT.MONTHLY_SUBSCRIPTION_SERVICE_FEE / 100                                                       AS MRR_AFTER_PAUSE,
       AFT.SUBSCRIPTION_CANCELLED_DATE::DATE                                                            AS CANCELLED_DATE_AFTER_PAUSE,
       current_date                                                                                     AS ASOF_DATE
FROM _KEYS_ K
    INNER JOIN {{ ref('stg_ams_account__account') }} A ON K.ACCOUNT_ID = A.ID
    LEFT JOIN _BEFORE_ BEF ON K.ACCOUNT_ID = BEF.ACCOUNT_ID
        AND K.EVENT_DATETIME = BEF.SUBSCRIPTION_CANCELLED_DATE
        AND K.PRODUCT = BEF.PRODUCT
        AND K.PLAN_CODE = BEF.PLAN_CODE
        AND K.SEQNUM = BEF.SEQNUM
    LEFT JOIN _AFTER_ AFT ON K.ACCOUNT_ID = AFT.ACCOUNT_ID
        AND K.EVENT_DATETIME = AFT.SUBSCRIPTION_CREATE_DATE
        AND K.PRODUCT = AFT.PRODUCT
        AND K.PLAN_CODE = AFT.PLAN_CODE
        AND K.SEQNUM = AFT.SEQNUM
