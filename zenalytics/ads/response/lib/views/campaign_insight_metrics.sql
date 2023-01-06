CREATE OR REPLACE VIEW CAMPAIGN_INSIGHT_METRICS COMMENT='IN DEVELOPMENT 2020-07-16 - unified ads reporting view for camapaign-level metrics such as impressions, clicks & spend reported daily'
AS
-- CREATE OR REPLACE TABLE ZENPROD.ADS_CAMPAIGNS.CAMPAIGN_INSIGHT_METRICS 
-- AS
-- generate all dates back 10 years
with all_dates_ as (
  SELECT DATEADD ( DAY, '-' || ROW_NUMBER() over ( ORDER BY NULL ) , DATEADD ( DAY, '+1',
    CURRENT_DATE() ) ) AS DATE
  FROM
    TABLE ( GENERATOR ( rowcount => ( 3650 ) ) )
)
-- calculate the first and last dates of insight_data attached to each campaign
, campaign_date_range_ as (
  select
    i.ad_account_id
    , i.campaign_id
    , min(try_to_date(breakdown_value)) as min_insight_date
    , max(try_to_date(breakdown_value)) as max_insight_date
    , max(iff(spend_cents > 0, try_to_date(breakdown_value), NULL)) as max_date_with_spend
  from zenalytics.ads_campaigns.insights i
  where breakdown_type = 'DAY' and insight_type = 'AGGREGATE'
  group by 1,2
)
-- values of insight_type column for join
, insight_types_ as (
  select *
  from (VALUES ('AGGREGATE')
                , ('UNIQUE')) as i (insight_type) 
)
-- create all combinations of date, ad account id, campaign_id and insight type
-- columns: date, ad_account_id, campaign_id, insight, min_insight_date, max_insight_date, insight_type
, campaign_dates_ as (
  select *
  from all_dates_ d
       , campaign_date_range_ cdr
       , insight_types_ it
  where d.date >= cdr.min_insight_date and d.date <= max_insight_date
  order by campaign_id, date
)
-- campaign insights w/ filled in missing dates
, insights_ as (
  select 
    cd.ad_account_id
    , cd.campaign_id
    , cd.insight_type
    , cd.date
    , ifnull(i.impressions,0) as impressions
    , ifnull(i.clicks,0) as clicks
    , ifnull(i.engagement,0) as engagements
    , ifnull(i.spend_cents/100,0) as platform_spend
    , cd.max_date_with_spend
    , i.insight_id
  from campaign_dates_ cd
  left join zenalytics.ads_campaigns.insights i 
    on cd.ad_account_id = i.ad_account_id
    and cd.campaign_id = i.campaign_id
    and cd.insight_type = i.insight_type
    and cd.date = try_to_date(i.breakdown_value)
    and i.breakdown_type = 'DAY'
  where date <= dateadd(days, 8, max_date_with_spend)
)
, zenreach_campaign_records_locations_ as (
    select *, row_number() over (partition by ZENREACH_CAMPAIGN_RECORDS_ID order by LOCATION_ID) as rank
    from zenalytics.ads_campaigns.zenreach_campaign_records_locations
)
, campaign_names_ as (
  select distinct campaign_id, name as campaign_name
  from zenalytics.ads_campaigns.campaigns c
)
, campaign_goals_ as (
  select
    zcr.campaign_id
  , zc.zenreach_campaign_id
  , zcr.platform
  , zc.name
  , zc.campaign_goal
  , zc.creation_source
  from zenalytics.ads_campaigns.zenreach_campaigns zc
  left join zenalytics.ads_campaigns.zenreach_campaign_records zcr
      on zc.zenreach_campaign_id = zcr.zenreach_campaign_id
  where zc.status <> 'FAILED' and zcr.platform = 'FACEBOOK'
)
, margins_ as (
  select
    *
    , lag(updated) over (partition by ZENREACH_CAMPAIGN_RECORDS_ID order by updated desc) as effective_through
  from zenalytics.ads_campaigns.zenreach_campaign_records_margins m
)
-- link clicks only available in ad-insights table. Aggregating from here, but only available after 2019-11-15.
, link_clicks_ as (
  select 
    campaign_id
    , insight_type
    , try_to_date(breakdown_value) as date
    , sum(link_clicks) as link_clicks
  from zenalytics.ads_campaigns.ad_insights
  where breakdown_type = 'DAILY'
  group by 1,2,3
)
, campaign_insight_metrics_ as (
  select 
    -- parent_id logic:
    --  (1) zenreach_campaign_record_locations joins to get businessprofile_hierarchy.parent
    --  (2) adsbiz parent_id, correcting entries at the group level to root parent_id in CTE adsbiz_
    --  (3) failing all that, use zenreach_campaign_record_locations.location_id as the parent and get the name from portal_businessprofile
    coalesce(
      bh.parent_id
      , zcrl.location_id
      , zaa.account_id) as parent_id
    , coalesce(
        bh.parent_name
        , bf.name
        , zaabp.name) as parent_name
    , i.ad_account_id
    , i.campaign_id
    , c.campaign_name as campaign_name
    , zc.campaign_goal as campaign_goal
    , i.insight_type
    , i.date
    , i.impressions as impressions
    , i.clicks as clicks
    , i.engagements as engagements
    , iff(i.insight_id is null and i.date > '2019-11-15', 0, lc.link_clicks) as link_clicks
    , m.margin_percent as margin
    , i.platform_spend
    , (i.platform_spend ) / (1 - ifnull(m.margin_percent,0)) as investment_dollars
    , i.insight_id
    , i.max_date_with_spend
    , current_timestamp() as updated_at
  from insights_ i
  left join zenalytics.ads_campaigns.zenreach_campaign_records zcr
    on i.campaign_id = zcr.campaign_id
  left join zenalytics.ads_campaigns.zenreach_campaigns zc
    on zcr.zenreach_campaign_id = zc.zenreach_campaign_id
  left join zenreach_campaign_records_locations_ zcrl
    on zcr.zenreach_campaign_records_id = zcrl.zenreach_campaign_records_id
  left join zenalytics.ads_campaigns.zenreach_ad_account_fb_config zaafc
    on zaafc.facebook_ad_account_id = i.ad_account_id
  left join zenalytics.ads_campaigns.zenreach_ad_accounts zaa
    on zaafc.ad_account_id = zaa.ad_account_id
  left join zenalytics.crm.portal_businessprofile zaabp
    on zaa.account_id = zaabp.business_id
  left join zenalytics.crm.businessprofile_hierarchy bh
    on zcrl.location_id = bh.business_id
  left join zenalytics.crm.portal_businessprofile bf
    on zcrl.location_id = bf.business_id
  left join campaign_names_ c on i.campaign_id = c.campaign_id
  join zenalytics.ads_campaigns.ad_accounts ac
    on i.ad_account_id = ac.ad_account_id
  left join campaign_goals_ g on c.campaign_id = g.campaign_id
  left join  margins_ m
          on zcr.ZENREACH_CAMPAIGN_RECORDS_ID = m.ZENREACH_CAMPAIGN_RECORDS_ID
          and i.date >= m.updated
          and i.date < ifnull(m.effective_through, current_date())
  left join link_clicks_ lc on i.campaign_id = lc.campaign_id and i.date = lc.date and i.insight_type = lc.insight_type
  where
    ac.is_zenreach = TRUE
    and zcr.platform = 'FACEBOOK'
    and zcrl.rank = 1
    and i.ad_account_id <> '126959897820002' -- `Zenreach Partner` ad account, used for testing circa 2018
    and i.ad_account_id <> '438192360194940' -- Brixx POS tests
    and i.ad_account_id <> '577138613087658' -- Brixx POS tests
    and i.ad_account_id <> '3288408101232413' -- POS tests
    and i.ad_account_id <> '349581652365682' -- Test Ad Level Reporting Video
    and datediff(days, i.date, max_date_with_spend) >= -8 -- stop pulling in campaigns that haven't had spend for a week - revisit if conversion window is adjusted
)

select *
from campaign_insight_metrics_;
