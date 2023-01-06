CREATE OR REPLACE VIEW AD_SUMMARY COMMENT='IN DEVELOPMENT 2020-07-16 - unified ads reporting view for ad-to-date summary metrics including frequency & reach'
AS
with ad_insight_metrics_ as (
    select 
    parent_id
    , parent_name
    , ad_account_id
    , campaign_id
    , campaign_name
    , campaign_goal
    , ad_set_id
    , ad_set_name
    , ad_id
    , ad_name
    , sum(impressions) as impressions
    , sum(link_clicks) as link_clicks
    , sum(engagements) as engagements
    , sum(platform_spend) as platform_spend
    , round(sum(INVESTMENT_DOLLARS),2) as investment_dollars
    , min(date) as min_date
    , max(date) as max_date
    , count(distinct iff(platform_spend > 0, date, NULL)) as days_live
    , count(distinct iff(platform_spend > 0 and margin is null, date, NULL)) as missing_margins
    , sum(distinct iff(platform_spend > 0 and margin is null, platform_spend/100, NULL)) as spend_missing_margins
  from ads_campaigns.ad_insight_metrics
  where insight_type = 'AGGREGATE'
  group by 1,2,3,4,5,6,7,8,9,10
)
, ad_walkthroughs_ as (
    select 
    ad_account_id
    , campaign_id
    , ad_set_id
    , ad_id
    , sum(CONFIRMED_WALKTHROUGHS) as confirmed_walkthroughs
    , sum(TOTAL_WALKTHROUGHS) as total_walkthroughs
  from ads_campaigns.ad_walkthroughs
  where insight_type = 'AGGREGATE'
  group by 1,2,3,4
)
select 
  am.PARENT_ID
  , am.PARENT_NAME
  , am.AD_ACCOUNT_ID
  , am.CAMPAIGN_ID
  , am.CAMPAIGN_NAME
  , am.CAMPAIGN_GOAL
  , ia.impressions
  , iu.impressions as reach
  , iff(iu.impressions > 0, round(ia.impressions / iu.impressions,1), NULL) as frequency
  , ia.CLICKS
  , am.LINK_CLICKS
  , ia.engagement as engagements
  , am.INVESTMENT_DOLLARS
  , am.platform_spend
  , aw.CONFIRMED_WALKTHROUGHS
  , aw.total_walkthroughs as total_walkthroughs
  , iff(aw.total_walkthroughs > 0, round(am.INVESTMENT_DOLLARS / aw.total_walkthroughs,2), NULL) as CPWT
  , round(iff(ia.impressions > 0, ia.CLICKS / ia.impressions , NULL),4) as CTR
  , iff(ia.impressions > 0, round(am.INVESTMENT_DOLLARS / (ia.impressions / 1000) , 2), NULL) as CPM
from ad_insight_metrics_ am
    , ad_walkthroughs_ aw
    , ZENALYTICS.ADS_CAMPAIGNS.INSIGHTS ia
    , ZENALYTICS.ADS_CAMPAIGNS.INSIGHTS iu
where ia.BREAKDOWN_TYPE = 'NONE' and ia.INSIGHT_TYPE = 'AGGREGATE'
      and iu.BREAKDOWN_TYPE = 'NONE' and iu.INSIGHT_TYPE = 'UNIQUE'
      and am.AD_ACCOUNT_ID = ia.AD_ACCOUNT_ID
      and am.CAMPAIGN_ID = ia.CAMPAIGN_ID
      and am.AD_ACCOUNT_ID = iu.AD_ACCOUNT_ID
      and am.CAMPAIGN_ID = iu.CAMPAIGN_ID
      and am.AD_ACCOUNT_ID = aw.AD_ACCOUNT_ID
      and am.CAMPAIGN_ID = aw.CAMPAIGN_ID
      and am.AD_SET_ID = aw.AD_SET_ID
      and am.AD_ID = aw.AD_ID;
