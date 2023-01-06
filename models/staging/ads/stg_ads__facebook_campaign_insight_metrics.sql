-- generate all dates back 10 years
WITH ALL_DATES_ AS (
    SELECT DATEADD(DAY, '-' || ROW_NUMBER() OVER ( ORDER BY NULL ), DATEADD(DAY, '+1',
                                                                            CURRENT_DATE())) AS DATE
    FROM
        TABLE ( GENERATOR(ROWCOUNT => (3650)) )
)
-- calculate the first and last dates of insight_data attached to each campaign
   , CAMPAIGN_DATE_RANGE_ AS (
    SELECT I.AD_ACCOUNT_ID
         , I.CAMPAIGN_ID
         , min(try_to_date(BREAKDOWN_VALUE))                                                       AS MIN_INSIGHT_DATE
         , max(try_to_date(BREAKDOWN_VALUE))                                                       AS MAX_INSIGHT_DATE
         , ifnull(max(iff(SPEND_CENTS > 0, try_to_date(BREAKDOWN_VALUE), NULL)),
                  MAX_INSIGHT_DATE)                                                                AS MAX_DATE_WITH_SPEND
    FROM {{ ref('stg_ads__insights') }} I
    WHERE BREAKDOWN_TYPE = 'DAY'
      AND INSIGHT_TYPE = 'AGGREGATE'
    GROUP BY 1, 2
)
-- values of insight_type column for join
   , INSIGHT_TYPES_ AS (
    SELECT *
    FROM (VALUES ('AGGREGATE')
               , ('UNIQUE')) AS I (INSIGHT_TYPE)
)
-- create all combinations of date, ad account id, campaign_id and insight type
-- columns: date, ad_account_id, campaign_id, insight, min_insight_date, max_insight_date, insight_type
   , CAMPAIGN_DATES_ AS (
    SELECT *
    FROM ALL_DATES_ D
       , CAMPAIGN_DATE_RANGE_ CDR
       , INSIGHT_TYPES_ IT
    WHERE D.DATE >= CDR.MIN_INSIGHT_DATE
      AND D.DATE <= MAX_INSIGHT_DATE
    ORDER BY CAMPAIGN_ID, DATE
)
-- campaign insights w/ filled in missing dates
   , INSIGHTS_ AS (
    SELECT CD.AD_ACCOUNT_ID
         , CD.CAMPAIGN_ID
         , CD.INSIGHT_TYPE
         , CD.DATE
         , ifnull(I.IMPRESSIONS, 0)       AS IMPRESSIONS
         , ifnull(I.CLICKS, 0)            AS CLICKS
         , ifnull(I.ENGAGEMENT, 0)        AS ENGAGEMENTS
         , ifnull(I.SPEND_CENTS / 100, 0) AS PLATFORM_SPEND
         , CD.MAX_DATE_WITH_SPEND
         , I.INSIGHT_ID
    FROM CAMPAIGN_DATES_ CD
             LEFT JOIN {{ ref('stg_ads__insights') }} I
                       ON CD.AD_ACCOUNT_ID = I.AD_ACCOUNT_ID
                           AND CD.CAMPAIGN_ID = I.CAMPAIGN_ID
                           AND CD.INSIGHT_TYPE = I.INSIGHT_TYPE
                           AND CD.DATE = try_to_date(I.BREAKDOWN_VALUE)
                           AND I.BREAKDOWN_TYPE = 'DAY'
    WHERE DATE <= dateadd(DAYS, 29, MAX_DATE_WITH_SPEND)
)
   , ZENREACH_CAMPAIGN_RECORDS_LOCATIONS_ AS (
    SELECT *, row_number() OVER (PARTITION BY ZENREACH_CAMPAIGN_RECORDS_ID ORDER BY LOCATION_ID) AS RANK
    FROM {{ ref('stg_ads__zenreach_campaign_record_locations') }}
)
   , CAMPAIGN_NAMES_ AS (
    SELECT DISTINCT CAMPAIGN_ID, NAME AS CAMPAIGN_NAME
    FROM {{ ref('stg_ads__campaigns') }} C
)
   , CAMPAIGN_GOALS_ AS (
    SELECT ZCR.CAMPAIGN_ID
         , ZC.ZENREACH_CAMPAIGN_ID
         , ZCR.PLATFORM
         , ZC.NAME
         , ZC.CAMPAIGN_GOAL
         , ZC.CREATION_SOURCE
    FROM {{ ref('stg_ads__zenreach_campaigns') }} ZC
             LEFT JOIN {{ ref('stg_ads__zenreach_campaign_records') }} ZCR
                       ON ZC.ZENREACH_CAMPAIGN_ID = ZCR.ZENREACH_CAMPAIGN_ID
    WHERE ZC.STATUS <> 'FAILED'
      AND ZCR.PLATFORM = 'FACEBOOK'
)
   , MARGINS_ AS (
    SELECT *
         , lag(UPDATED) OVER (PARTITION BY ZENREACH_CAMPAIGN_RECORDS_ID ORDER BY UPDATED DESC) AS EFFECTIVE_THROUGH
    FROM {{ ref('stg_ads__zenreach_campaign_record_margins') }} M
)
-- link clicks only available in ad-insights table. Aggregating from here, but only available after 2019-11-15.
   , LINK_CLICKS_ AS (
    SELECT CAMPAIGN_ID
         , INSIGHT_TYPE
         , try_to_date(BREAKDOWN_VALUE) AS DATE
         , sum(LINK_CLICKS)             AS LINK_CLICKS
    FROM {{ ref('stg_ads__ad_insights') }}
    WHERE BREAKDOWN_TYPE = 'DAILY'
    GROUP BY 1, 2, 3
)
   , ZENREACH_AD_ACCOUNT_CAMPAIGNS_ AS (
    SELECT ZADC.ZENREACH_CAMPAIGN_ID
    FROM {{ ref('stg_ads__zenreach_ad_account_campaigns') }} ZADC
             JOIN {{ ref('stg_ads__zenreach_ad_accounts') }} ZAD
                  ON ZADC.AD_ACCOUNT_ID = ZAD.AD_ACCOUNT_ID
    WHERE PLATFORM = 'FACEBOOK'
)
   , CAMPAIGN_INSIGHT_METRICS_ AS (
    SELECT
         -- parent_id logic:
         --  (1) zenreach_campaign_record_locations joins to get businessprofile_hierarchy.parent
         --  (2) adsbiz parent_id, correcting entries at the group level to root parent_id in CTE adsbiz_
         --  (3) failing all that, use zenreach_campaign_record_locations.location_id as the parent and get the name from portal_businessprofile
        coalesce(
                BH.PARENT_ID
            , ZCRL.LOCATION_ID
            , ZAA.ACCOUNT_ID)                                                     AS PARENT_ID
         , coalesce(
            BH.PARENT_NAME
        , BF.NAME
        , ZAABP.NAME)                                                             AS PARENT_NAME
         , I.AD_ACCOUNT_ID
         , I.CAMPAIGN_ID
         , C.CAMPAIGN_NAME                                                        AS CAMPAIGN_NAME
         , ZC.CAMPAIGN_GOAL                                                       AS CAMPAIGN_GOAL
         , I.INSIGHT_TYPE
         , I.DATE
         , I.IMPRESSIONS                                                          AS IMPRESSIONS
         , I.CLICKS                                                               AS CLICKS
         , I.ENGAGEMENTS                                                          AS ENGAGEMENTS
         , iff(I.INSIGHT_ID IS NULL AND I.DATE > '2019-11-15', 0, LC.LINK_CLICKS) AS LINK_CLICKS
         , M.MARGIN_PERCENT                                                       AS MARGIN
         , I.PLATFORM_SPEND
         , (I.PLATFORM_SPEND) / (1 - ifnull(M.MARGIN_PERCENT, 0))                 AS INVESTMENT_DOLLARS
         , I.INSIGHT_ID
         , I.MAX_DATE_WITH_SPEND
         , current_timestamp()                                                    AS UPDATED_AT
    FROM INSIGHTS_ I
             LEFT JOIN {{ ref('stg_ads__zenreach_campaign_records') }} ZCR
                       ON I.CAMPAIGN_ID = ZCR.CAMPAIGN_ID
             LEFT JOIN {{ ref('stg_ads__zenreach_campaigns') }} ZC
                       ON ZCR.ZENREACH_CAMPAIGN_ID = ZC.ZENREACH_CAMPAIGN_ID
             LEFT JOIN ZENREACH_CAMPAIGN_RECORDS_LOCATIONS_ ZCRL
                       ON ZCR.ZENREACH_CAMPAIGN_RECORDS_ID = ZCRL.ZENREACH_CAMPAIGN_RECORDS_ID
             LEFT JOIN {{ ref('stg_ads__zenreach_ad_account_FB_configurations') }} ZAAFC
                       ON ZAAFC.FACEBOOK_AD_ACCOUNT_ID = I.AD_ACCOUNT_ID
             LEFT JOIN {{ ref('stg_ads__zenreach_ad_accounts') }} ZAA
                       ON ZAAFC.AD_ACCOUNT_ID = ZAA.AD_ACCOUNT_ID
             LEFT JOIN {{ ref('stg_crm__portal_businessprofile') }} ZAABP
                       ON ZAA.ACCOUNT_ID = ZAABP.BUSINESS_ID
             LEFT JOIN {{ ref('stg_crm__businessprofile_hierarchy') }} BH
                       ON ZCRL.LOCATION_ID = BH.BUSINESS_ID
             LEFT JOIN {{ ref('stg_crm__portal_businessprofile') }} BF
                       ON ZCRL.LOCATION_ID = BF.BUSINESS_ID
             LEFT JOIN CAMPAIGN_NAMES_ C ON I.CAMPAIGN_ID = C.CAMPAIGN_ID
             JOIN {{ ref('stg_ads__accounts') }} AC
                  ON I.AD_ACCOUNT_ID = AC.AD_ACCOUNT_ID
             LEFT JOIN CAMPAIGN_GOALS_ G ON C.CAMPAIGN_ID = G.CAMPAIGN_ID
             LEFT JOIN MARGINS_ M
                       ON ZCR.ZENREACH_CAMPAIGN_RECORDS_ID = M.ZENREACH_CAMPAIGN_RECORDS_ID
                           AND I.DATE >= M.UPDATED
                           AND I.DATE < ifnull(M.EFFECTIVE_THROUGH, current_date())
             LEFT JOIN LINK_CLICKS_ LC
                       ON I.CAMPAIGN_ID = LC.CAMPAIGN_ID AND I.DATE = LC.DATE AND I.INSIGHT_TYPE = LC.INSIGHT_TYPE
    WHERE AC.IS_ZENREACH = TRUE
      AND ZCR.PLATFORM = 'FACEBOOK'
      AND (
            ZCRL.RANK = 1
            OR (
                -- locationless campaign
                    ZCRL.RANK IS NULL
                    AND ZCRL.LOCATION_ID IS NULL
                    AND ZCR.ZENREACH_CAMPAIGN_ID IN
                        (SELECT ZENREACH_CAMPAIGN_ID FROM ZENREACH_AD_ACCOUNT_CAMPAIGNS_) -- is on a Zenreach Ad Account
                )
        )
      AND I.AD_ACCOUNT_ID <> '126959897820002'  -- `Zenreach Partner` ad account, used for testing circa 2018
      AND I.AD_ACCOUNT_ID <> '438192360194940'  -- Brixx POS tests
      AND I.AD_ACCOUNT_ID <> '577138613087658'  -- Brixx POS tests
      AND I.AD_ACCOUNT_ID <> '3288408101232413' -- POS tests
      AND I.AD_ACCOUNT_ID <> '349581652365682'  -- Test Ad Level Reporting Video
      AND datediff(DAYS, I.DATE, MAX_DATE_WITH_SPEND) >= -29 -- stop pulling in campaigns that haven't had spend for 28 days - revisit if conversion window is adjusted
)

SELECT *
FROM CAMPAIGN_INSIGHT_METRICS_