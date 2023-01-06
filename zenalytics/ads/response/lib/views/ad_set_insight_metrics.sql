CREATE OR REPLACE VIEW AD_SET_INSIGHT_METRICS COMMENT='IN DEVELOPMENT 2020-07-16 - unified ads reporting view for ad set level metrics such as impressions, clicks & spend reported daily'
AS
with all_dates_ as (
  SELECT DATEADD ( DAY, '-' || ROW_NUMBER() over ( ORDER BY NULL ) , DATEADD ( DAY, '+1',
    CURRENT_DATE() ) ) AS DATE
  FROM
    TABLE ( GENERATOR ( rowcount => ( 3650 ) ) )
)
-- calculate the first and last dates of insight_data attached to each campaign
, ad_set_date_range_ as (
  select
    i.ad_account_id
    , i.campaign_id
    , i.ad_set_id
    , min(try_to_date(breakdown_value)) as min_insight_date
    , max(try_to_date(breakdown_value)) as max_insight_date
    , max(iff(spend_cents > 0, try_to_date(breakdown_value), NULL)) as max_date_with_spend
  from zenalytics.ads_campaigns.ad_set_insights i
  where breakdown_type = 'DAILY' and insight_type = 'AGGREGATE'
  group by 1,2,3
)
-- values of insight_type column for join
, insight_types_ as (
  select *
  from (VALUES ('AGGREGATE')
                , ('UNIQUE')) as i (insight_type) 
)
-- create all combinations of date, ad account id, campaign_id and insight type
-- columns: date, ad_account_id, campaign_id, insight, min_insight_date, max_insight_date, insight_type
, ad_set_dates_ as (
  select *
  from all_dates_ d
       , ad_set_date_range_ cdr
       , insight_types_ it
  where 
    d.date >= cdr.min_insight_date 
    and d.date <= max_insight_date
    -- ad set level data isn't completely populated earlier in 2019
    -- when we started pulling this data active ads backfilled but inactive did not
    -- this can lead to confusion / inconsistencies when summing ad set level data for accounts
    -- prior to the time we started acquiring this data from FB on all live accounts (roughtly Nov 2019)
    and d.date >= '2019-11-15'
  order by campaign_id, date
)
-- campaign insights w/ filled in missing dates
, insights_ as (
  select 
    ads.ad_account_id
    , ads.campaign_id
    , ads.ad_set_id
    , ads.insight_type
    , ads.date
    , ifnull(i.impressions,0) as impressions
    , ifnull(i.clicks,0) as clicks
    , ifnull(i.link_clicks,0) as link_clicks
    , ifnull(i.engagement,0) as engagements
    , ifnull(i.spend_cents/100,0) as platform_spend
    , ads.max_date_with_spend
    , i.insight_id
  from ad_set_dates_ ads
  left join zenalytics.ads_campaigns.ad_set_insights i 
    on ads.ad_account_id = i.ad_account_id
    and ads.campaign_id = i.campaign_id
    and ads.ad_set_id = i.ad_set_id
    and ads.insight_type = i.insight_type
    and ads.date = try_to_date(i.breakdown_value)
    and i.breakdown_type = 'DAILY'
  where date <= dateadd(days, 8, max_date_with_spend)
)
-- the rank is used to find a single loc for a multi-loc campaign to use to walk back to the parent_id
, zenreach_campaign_records_locations_ as (
    select *, row_number() over (partition by ZENREACH_CAMPAIGN_RECORDS_ID order by LOCATION_ID) as rank
    from zenalytics.ads_campaigns.zenreach_campaign_records_locations
)
, campaign_names_ as (
  select distinct campaign_id, name as campaign_name
  from zenalytics.ads_campaigns.campaigns
)
, ad_set_names_ as (
  select distinct ad_set_id, name as ad_set_name
  from zenalytics.ads_campaigns.ad_sets
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
, ad_set_insight_metrics_ as (
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
    , i.ad_set_id
    , asn.ad_set_name
    , i.insight_type
    , i.date
    , i.impressions as impressions
    , i.clicks as clicks
    , i.link_clicks as link_clicks
    , i.engagements
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
  left join ad_set_names_ asn on i.ad_set_id = asn.ad_set_id
  join zenalytics.ads_campaigns.ad_accounts ac
    on i.ad_account_id = ac.ad_account_id
  left join campaign_goals_ g on c.campaign_id = g.campaign_id
  left join  margins_ m
          on zcr.ZENREACH_CAMPAIGN_RECORDS_ID = m.ZENREACH_CAMPAIGN_RECORDS_ID
          and i.date >= m.updated
          and i.date < ifnull(m.effective_through, current_date())
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
from ad_set_insight_metrics_;
