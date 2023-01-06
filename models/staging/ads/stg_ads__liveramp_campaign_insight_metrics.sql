WITH LR_CAMPAIGN_INSIGHT_METRICS AS (
    SELECT PARENT_ID
         , PARENT_NAME
         , AD_ACCOUNT_ID
         , ZENREACH_CAMPAIGN_ID
         , ZENREACH_CAMPAIGN_RECORDS_ID
         , CAMPAIGN_UUID
         , CAMPAIGN_ID
         , CAMPAIGN_NAME
         , CAMPAIGN_GOAL
         , INSIGHT_TYPE
         , DATE
         , MAX_DATE_WITH_SPEND
         , MARGIN
         , current_timestamp()     AS UPDATED_AT
         , sum(IMPRESSIONS)        AS IMPRESSIONS
         , sum(CLICKS)             AS CLICKS
         , sum(PLATFORM_SPEND)     AS PLATFORM_SPEND
         , sum(INVESTMENT_DOLLARS) AS INVESTMENT_DOLLARS
    FROM {{ ref('stg_ads__liveramp_ad_insight_metrics') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
)

SELECT *
FROM LR_CAMPAIGN_INSIGHT_METRICS

