{{ config(materialized='table') }}
-- get parent_ids and names from CRM hiearchy
WITH PARENTS_ AS (
    SELECT DISTINCT PARENT_ID
                  , PARENT_NAME
    FROM {{ ref('stg_crm__businessprofile_hierarchy') }}
)

-- get all zenreach liveramp ad accounts and include the parent info
   , LIVERAMP_AD_ACCOUNTS_ AS (
    SELECT ACCOUNT_ID    AS PARENT_ID
         , P.PARENT_NAME AS PARENT_NAME
         , AD_ACCOUNT_ID
    FROM {{ ref('stg_ads__zenreach_ad_accounts') }} ZAA
             INNER JOIN PARENTS_ P
                        ON ZAA.ACCOUNT_ID = P.PARENT_ID
    WHERE PLATFORM = 'LIVERAMP'
)

-- get all campaign info down to the creative level
   , LIVERAMP_ZENREACH_CAMPAIGNS_ AS (
    SELECT ZAA.ACCOUNT_ID                      AS PARENT_ID
         , P.PARENT_NAME
         , ZAA.AD_ACCOUNT_ID
         , ZAAC.ZENREACH_CAMPAIGN_ID
         , ZCR.ZENREACH_CAMPAIGN_RECORDS_ID
         , LRC.ID                              AS CAMPAIGN_UUID
         , LRC.CAMPAIGN_ID
         , LRC.NAME                            AS CAMPAIGN_NAME
         , ZC.CAMPAIGN_GOAL
         , LRAG.ID                             AS AD_GROUP_UUID
         , LRAG.AD_GROUP_ID
         , LRAG.NAME                           AS AD_GROUP_NAME
         , ifnull(ZAAG.GOAL, ZC.CAMPAIGN_GOAL) AS AD_GROUP_GOAL
         , LRCR.ID                             AS CREATIVE_UUID
         , LRCR.CREATIVE_ID
         , LRCR.NAME                           AS CREATIVE_NAME
    FROM {{ ref('stg_ads__zenreach_ad_accounts') }} ZAA
             INNER JOIN PARENTS_ P
                        ON ZAA.ACCOUNT_ID = P.PARENT_ID
             INNER JOIN {{ ref('stg_ads__zenreach_ad_account_campaigns') }} ZAAC
                        ON ZAA.AD_ACCOUNT_ID = ZAAC.AD_ACCOUNT_ID
             INNER JOIN {{ ref('stg_ads__zenreach_campaigns') }} ZC
                        ON ZAAC.ZENREACH_CAMPAIGN_ID = ZC.ZENREACH_CAMPAIGN_ID
             INNER JOIN {{ ref('stg_ads__zenreach_campaign_records') }} ZCR
                        ON ZC.ZENREACH_CAMPAIGN_ID = ZCR.ZENREACH_CAMPAIGN_ID
             INNER JOIN {{ ref('stg_ads__liveramp_campaigns') }} LRC
                        ON ZCR.CAMPAIGN_ID = LRC.CAMPAIGN_ID
             INNER JOIN {{ ref('stg_ads__liveramp_ad_groups') }} LRAG
                        ON LRC.ID = LRAG.CAMPAIGN_ID
             INNER JOIN {{ ref('stg_ads__liveramp_creatives') }} LRCR
                        ON LRAG.ID = LRCR.AD_GROUP_ID
             LEFT JOIN {{ ref('stg_ads__zenreach_ad_set_goals') }} ZAAG
                       ON ZCR.ZENREACH_CAMPAIGN_RECORDS_ID = ZAAG.ZENREACH_CAMPAIGN_RECORDS_ID
                           AND LRAG.AD_GROUP_ID = ZAAG.AD_SET_ID

    WHERE ZAA.PLATFORM = 'LIVERAMP'
      AND ZC.STATUS = 'CREATED'
      AND ZCR.CREATION_STATUS = 'CREATED'
)

-- generate all dates back 10 years
   , ALL_DATES_ AS (
    SELECT DATEADD(DAY, '-' || ROW_NUMBER() OVER ( ORDER BY NULL ), DATEADD(DAY, '+1',
                                                                            CURRENT_DATE())) AS DATE
    FROM
        TABLE ( GENERATOR(ROWCOUNT => (3650)) )
)

-- get all dates that the campaign has metrics and spend for
   , CAMPAIGN_DATES_ AS (
    SELECT LZC.CAMPAIGN_UUID
         , LIC.DATE
         , sum(LIC.COST_CENTS) AS COST_CENTS
    FROM {{ ref('stg_ads__liveramp_insight_creatives') }} LIC
             INNER JOIN LIVERAMP_ZENREACH_CAMPAIGNS_ LZC
                        ON LIC.CREATIVE_ID = LZC.CREATIVE_UUID
    GROUP BY 1, 2
)

-- get all dates with walkthroughs
   , WALKTHROUGH_DATES_ AS (
    SELECT LZC.CAMPAIGN_UUID
         , LIW.SIGHTING_DAY AS DATE
         , 0                AS COST_CENTS
    FROM {{ ref('stg_ads__liveramp_daily_walkthrough_impressions') }} LIW
             INNER JOIN LIVERAMP_ZENREACH_CAMPAIGNS_ LZC
                        ON LIW.CREATIVE_ID = LZC.CREATIVE_UUID
    GROUP BY 1, 2
)

-- combine the campaign running dates and walkthrough dates
   , LIVERAMP_CAMPAIGN_DATE_RANGE_ AS (
    SELECT CAMPAIGN_UUID
         , min(try_to_date(DATE))                                                      AS MIN_INSIGHT_DATE
         , max(try_to_date(DATE))                                                      AS MAX_INSIGHT_DATE
         , ifnull(max(iff(COST_CENTS > 0, try_to_date(DATE), NULL)), MAX_INSIGHT_DATE) AS MAX_DATE_WITH_SPEND
    FROM (SELECT * FROM CAMPAIGN_DATES_ UNION SELECT * FROM WALKTHROUGH_DATES_)
    GROUP BY 1
)

-- generate creative date range based on the campaign running dates
   , LIVERAMP_CREATIVE_DATE_RANGE_ AS (
    SELECT LCDR.CAMPAIGN_UUID
         , CREATIVE_UUID
         , LCDR.MIN_INSIGHT_DATE
         , LCDR.MAX_INSIGHT_DATE
         , LCDR.MAX_DATE_WITH_SPEND
    FROM LIVERAMP_CAMPAIGN_DATE_RANGE_ LCDR
             INNER JOIN LIVERAMP_ZENREACH_CAMPAIGNS_ LZC
                        ON LCDR.CAMPAIGN_UUID = LZC.CAMPAIGN_UUID
)

-- values of insight_type column for join
   , LIVERAMP_INSIGHT_TYPES_ AS (
    SELECT *
    FROM (VALUES ('AGGREGATE')) AS I (INSIGHT_TYPE)
)

-- generate all liveramp ad dates based on ranges defined above
   , LIVERAMP_AD_DATES_ AS (
    SELECT *
    FROM ALL_DATES_ D
       , LIVERAMP_CREATIVE_DATE_RANGE_ CDR
       , LIVERAMP_INSIGHT_TYPES_ IT
    WHERE D.DATE >= CDR.MIN_INSIGHT_DATE
      AND D.DATE <= MAX_INSIGHT_DATE
    ORDER BY CREATIVE_UUID, DATE
)

-- get margins for all campaigns
   , MARGINS_ AS (
    SELECT *
         , lag(UPDATED) OVER (PARTITION BY ZENREACH_CAMPAIGN_RECORDS_ID ORDER BY UPDATED DESC) AS EFFECTIVE_THROUGH
    FROM {{ ref('stg_ads__zenreach_campaign_record_margins') }} M
)

-- generate ad insights for all liveramp campaings
   , LIVERAMP_INSIGHTS_ AS (
    SELECT LZC.PARENT_ID
         , LZC.PARENT_NAME
         , LZC.AD_ACCOUNT_ID
         , LZC.ZENREACH_CAMPAIGN_ID
         , LZC.ZENREACH_CAMPAIGN_RECORDS_ID
         , LZC.CAMPAIGN_UUID
         , LZC.CAMPAIGN_ID
         , LZC.CAMPAIGN_NAME
         , LZC.CAMPAIGN_GOAL
         , LZC.AD_GROUP_UUID
         , LZC.AD_GROUP_ID
         , LZC.AD_GROUP_NAME
         , LZC.AD_GROUP_GOAL
         , LZC.CREATIVE_UUID
         , LZC.CREATIVE_ID
         , LZC.CREATIVE_NAME
         , LAD.INSIGHT_TYPE
         , LAD.DATE
         , LAD.MAX_DATE_WITH_SPEND
         , current_timestamp()                        AS UPDATED_AT
         , sum(CAST(ifnull(I.IMPRESSIONS, 0) AS INT)) AS IMPRESSIONS
         , sum(CAST(ifnull(I.CLICKS, 0) AS INT))      AS CLICKS
         , sum(ifnull(I.COST_CENTS / 100, 0))         AS PLATFORM_SPEND
    FROM LIVERAMP_AD_DATES_ LAD
             INNER JOIN LIVERAMP_ZENREACH_CAMPAIGNS_ LZC
                        ON LAD.CREATIVE_UUID = LZC.CREATIVE_UUID
             LEFT JOIN {{ ref('stg_ads__liveramp_insight_creatives') }} I
                       ON LZC.CREATIVE_UUID = I.CREATIVE_ID
                           AND LAD.DATE = I.DATE
    WHERE LAD.DATE <= dateadd(DAYS, 29, MAX_DATE_WITH_SPEND)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
)

   , LIVERAMP_INSIGHTS_WITH_MARGIN_APPLIED_ AS (
    SELECT LI.*
         , M.MARGIN_PERCENT                                     AS MARGIN
         , (PLATFORM_SPEND) / (1 - ifnull(M.MARGIN_PERCENT, 0)) AS INVESTMENT_DOLLARS
    FROM LIVERAMP_INSIGHTS_ LI
             LEFT JOIN MARGINS_ M
                       ON LI.ZENREACH_CAMPAIGN_RECORDS_ID = M.ZENREACH_CAMPAIGN_RECORDS_ID
                           AND LI.DATE >= M.UPDATED
                           AND LI.DATE < ifnull(M.EFFECTIVE_THROUGH, current_date())
)

SELECT *
FROM LIVERAMP_INSIGHTS_WITH_MARGIN_APPLIED_