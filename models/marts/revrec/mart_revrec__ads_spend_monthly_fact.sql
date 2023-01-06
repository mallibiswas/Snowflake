WITH MONTH_LIST AS (SELECT REPORT_DATE AS REPORT_MONTH
                    FROM {{ ref('mart_revrec__d_date') }}
                    WHERE DAY_OF_MON = 1 AND YEAR >= 2020 AND YEAR <= 2023),
     PARENT_LIST AS (SELECT DISTINCT PARENT_ID, PARENT_NAME
                     FROM {{ ref('stg_ads__campaign_insight_metrics') }}
                     WHERE INSIGHT_TYPE = 'AGGREGATE'),
     MONTH_TS AS (SELECT PARENT_ID, PARENT_NAME, REPORT_MONTH
                  FROM MONTH_LIST,
                       PARENT_LIST),
     SPEND_TS AS (
         SELECT M.PARENT_ID,
                M.PARENT_NAME,
                date_trunc('month', DATE)                                                           AS SPEND_MONTH, -- monthly spend
                count(DISTINCT CASE WHEN CHANNEL = 'FB' THEN CAMPAIGN_ID ELSE NULL END)             AS NUM_FB_CAMPAIGNS,
                count(DISTINCT CASE WHEN CHANNEL = 'LR' THEN CAMPAIGN_ID ELSE NULL END)             AS NUM_LR_CAMPAIGNS,
                sum(CASE WHEN CHANNEL = 'FB' THEN PLATFORM_SPEND ELSE 0 END)                        AS PLATFORM_SPEND_FB,
                sum(CASE WHEN CHANNEL = 'LR' THEN PLATFORM_SPEND ELSE 0 END)                        AS PLATFORM_SPEND_LR,
                sum(CASE WHEN CHANNEL = 'FB' THEN PLATFORM_SPEND / nvl((1 - MARGIN), 1) ELSE 0 END) AS CLIENT_SPEND_FB,
                sum(CASE WHEN CHANNEL = 'LR' THEN PLATFORM_SPEND / nvl((1 - MARGIN), 1) ELSE 0 END) AS CLIENT_SPEND_LR,
                sum(PLATFORM_SPEND)                                                                 AS PLATFORM_SPEND,
                sum(PLATFORM_SPEND / nvl((1 - MARGIN), 1))                                          AS CLIENT_SPEND -- client spend
         FROM {{ ref('stg_ads__campaign_insight_metrics') }} M
         WHERE INSIGHT_TYPE = 'AGGREGATE'
         GROUP BY 1, 2, 3
     )
SELECT M.PARENT_ID,
       M.PARENT_NAME,
       M.REPORT_MONTH            AS SPEND_MONTH,
       NVL(NUM_FB_CAMPAIGNS, 0)  AS NUM_FB_CAMPAIGNS,
       NVL(NUM_LR_CAMPAIGNS, 0)  AS NUM_LR_CAMPAIGNS,
       NVL(PLATFORM_SPEND_FB, 0) AS PLATFORM_SPEND_FB,
       NVL(PLATFORM_SPEND_LR, 0) AS PLATFORM_SPEND_LR,
       NVL(CLIENT_SPEND_FB, 0)   AS CLIENT_SPEND_FB,
       NVL(CLIENT_SPEND_LR, 0)   AS CLIENT_SPEND_LR,
       NVL(S.PLATFORM_SPEND, 0)  AS PLATFORM_SPEND,
       NVL(S.CLIENT_SPEND, 0)    AS CLIENT_SPEND
FROM MONTH_TS M
         LEFT JOIN SPEND_TS S ON M.PARENT_ID = S.PARENT_ID AND M.REPORT_MONTH = S.SPEND_MONTH
