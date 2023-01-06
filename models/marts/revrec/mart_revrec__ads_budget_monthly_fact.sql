WITH BUDGET AS (
    SELECT DISTINCT B.ACCOUNT__C                                        AS SFDC_ACCOUNT,
                    replace(M.PARENT_BID__C, ' ', '')                   AS PARENT_ID,
                    nvl(H.PARENT_NAME, B.ACCOUNT__C)                    AS PARENT_NAME,
                    C.NAME,
                    B.BUDGET_START_DATE__C::DATE                        AS BUDGET_START_DATE,
                    B.BUDGET_END_DATE__C::DATE                          AS BUDGET_END_DATE,
                    BUDGET_PERIOD_DAYS__C,
                    CASE
                        WHEN date_trunc(MONTH, BUDGET_START_DATE) =
                             date_trunc(MONTH, current_date) -- budget period in the current month
                            THEN DATEDIFF(DAY, BUDGET_START_DATE, LEAST(BUDGET_END_DATE, current_date)) + 1
                        WHEN date_trunc(MONTH, BUDGET_START_DATE) > date_trunc(MONTH, current_date) THEN 0
                        ELSE BUDGET_PERIOD_DAYS__C END                  AS BUDGET_DAYS, -- for current month, calculate # of days passed as budget period
                    B.BUDGET__C,
                    NVL(B.BUDGET__C, 0) / NVL(BUDGET_PERIOD_DAYS__C, 1) AS DAILY_BUDGET
    FROM {{ ref('stg_sfdc__budget__c') }} B
             LEFT JOIN {{ ref('stg_sfdc__advertising_campaign__c') }} C
                       ON B.ADVERTISING_CAMPAIGN__C = C.ID
             LEFT JOIN {{ ref('stg_sfdc__account_management__c') }} M
                       ON M.ID = C.AMO__C
             LEFT JOIN {{ ref('stg_crm__businessprofile_hierarchy') }} H
                       ON H.PARENT_ID = M.PARENT_BID__C
    WHERE B.ACCOUNT__C != 'ZENREACH OPERATIONS'
      AND B.ISDELETED = FALSE
      AND C.CAMPAIGN_STATUS__C != 'Onboarding'
)
SELECT SFDC_ACCOUNT,
       NVL(PARENT_ID, 'Unknown')            AS PARENT_ID,
       PARENT_NAME,
       date_trunc(MONTH, BUDGET_START_DATE) AS BUDGET_MONTH,
       sum(DAILY_BUDGET * BUDGET_DAYS)      AS PRORATED_BUDGET,
       sum(BUDGET__C)                       AS BUDGET,
       current_date                         AS ASOF_DATE
FROM BUDGET B
GROUP BY 1, 2, 3, 4
ORDER BY 1, 4
