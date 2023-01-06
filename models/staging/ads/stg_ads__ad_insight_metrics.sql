{{ config(materialized='table') }}
    WITH FACEBOOK_AD_INSIGHT_METRICS_ AS (
    SELECT PARENT_ID
         , PARENT_NAME
         , AD_ACCOUNT_ID
         , CAMPAIGN_ID
         , CAMPAIGN_NAME
         , CAMPAIGN_GOAL
         , AD_SET_ID
         , AD_SET_NAME
         , AD_ID
         , AD_NAME
         , INSIGHT_TYPE
         , DATE
         , IMPRESSIONS
         , CLICKS
         , LINK_CLICKS
         , ENGAGEMENTS
         , MARGIN
         , PLATFORM_SPEND
         , INVESTMENT_DOLLARS
         , INSIGHT_ID
         , MAX_DATE_WITH_SPEND
         , UPDATED_AT
    FROM {{ ref('stg_ads__facebook_ad_insight_metrics') }}
)

   , LIVERAMP_AD_INSIGHT_METRICS_ AS (
    SELECT PARENT_ID
         , PARENT_NAME
         , AD_ACCOUNT_ID
         , CAMPAIGN_ID
         , CAMPAIGN_NAME
         , CAMPAIGN_GOAL
         , AD_GROUP_ID   AS AD_SET_ID
         , AD_GROUP_NAME AS AD_SET_NAME
         , CREATIVE_ID   AS AD_ID
         , CREATIVE_NAME AS AD_NAME
         , INSIGHT_TYPE
         , DATE
         , IMPRESSIONS
         , CLICKS
         , CLICKS        AS LINK_CLICKS
         , 0             AS ENGAGEMENTS
         , MARGIN
         , PLATFORM_SPEND
         , INVESTMENT_DOLLARS
         , NULL          AS INSIGHT_ID
         , MAX_DATE_WITH_SPEND
         , UPDATED_AT
    FROM {{ ref('stg_ads__liveramp_ad_insight_metrics') }}
)

SELECT *
FROM FACEBOOK_AD_INSIGHT_METRICS_
UNION ALL
SELECT *
FROM LIVERAMP_AD_INSIGHT_METRICS_

