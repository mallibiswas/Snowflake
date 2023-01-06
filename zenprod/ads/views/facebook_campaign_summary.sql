CREATE OR REPLACE VIEW ZENPROD.ADS.FACEBOOK_CAMPAIGN_SUMMARY COMMENT='IN DEVELOPMENT 2020-07-16 - unified ads reporting view for campaign-to-date summary metrics including frequency & reach'
AS
with campaign_insight_metrics_ as (
    select 
    parent_id
    , parent_name
    , ad_account_id
    , campaign_id
    , campaign_name
    , campaign_goal
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
  from zenprod.ads.facebook_campaign_insight_metrics
  where insight_type = 'AGGREGATE'
  group by 1,2,3,4,5,6
)

, campaign_walkthroughs_ as (
    select 
    ad_account_id
    , campaign_id
    , sum(CONFIRMED_WALKTHROUGHS) as confirmed_walkthroughs
    , sum(TOTAL_WALKTHROUGHS) as total_walkthroughs
    , sum(confirmed_walkthroughs_1_day) as confirmed_walkthroughs_1_day
    , sum(total_walkthroughs_1_day) as total_walkthroughs_1_day
    , sum(confirmed_walkthroughs_7_day) as confirmed_walkthroughs_7_day
    , sum(total_walkthroughs_7_day) as total_walkthroughs_7_day
    , sum(confirmed_walkthroughs_28_day) as confirmed_walkthroughs_28_day
    , sum(total_walkthroughs_28_day) as total_walkthroughs_28_day
  from zenprod.ads.facebook_campaign_walkthroughs
  where insight_type = 'AGGREGATE'
  group by 1,2
)

select 
  cm.PARENT_ID
  , cm.PARENT_NAME
  , cm.AD_ACCOUNT_ID
  , cm.CAMPAIGN_ID
  , cm.CAMPAIGN_NAME
  , cm.CAMPAIGN_GOAL
  , ia.impressions
  , iu.impressions as reach
  , iff(iu.impressions > 0, round(ia.impressions / iu.impressions,1), NULL) as frequency
  , ia.CLICKS
  , cm.LINK_CLICKS
  , ia.engagement as engagements
  , cm.INVESTMENT_DOLLARS
  , cm.platform_spend
  , coalesce(cw.CONFIRMED_WALKTHROUGHS, 0) as CONFIRMED_WALKTHROUGHS
  , coalesce(cw.total_walkthroughs, 0) as total_walkthroughs
  , iff(cw.total_walkthroughs > 0, round(cm.INVESTMENT_DOLLARS / cw.total_walkthroughs,2), NULL) as CPWT
  , round(iff(ia.impressions > 0, ia.CLICKS / ia.impressions , NULL),4) as CTR
  , iff(ia.impressions > 0, round(cm.INVESTMENT_DOLLARS / (ia.impressions / 1000) , 2), NULL) as CPM
  , cm.days_live 
  , cm.min_date
  , cm.max_date
  , case when cm.max_date = current_date() then TRUE else FALSE end as active
  , cm.missing_margins
  , cm.spend_missing_margins
  , coalesce(cw.confirmed_walkthroughs_1_day, 0) as confirmed_walkthroughs_1_day
  , coalesce(cw.total_walkthroughs_1_day, 0) as total_walkthroughs_1_day
  , iff(cw.total_walkthroughs_1_day > 0, round(cm.INVESTMENT_DOLLARS / cw.total_walkthroughs_1_day,2), NULL) as CPWT_1_day
  , coalesce(cw.confirmed_walkthroughs_7_day, 0) as confirmed_walkthroughs_7_day
  , coalesce(cw.total_walkthroughs_7_day, 0) as total_walkthroughs_7_day
  , iff(cw.total_walkthroughs_7_day > 0, round(cm.INVESTMENT_DOLLARS / cw.total_walkthroughs_7_day,2), NULL) as CPWT_7_day
  , coalesce(cw.confirmed_walkthroughs_28_day, 0) as confirmed_walkthroughs_28_day
  , coalesce(cw.total_walkthroughs_28_day, 0) as total_walkthroughs_28_day
  , iff(cw.total_walkthroughs_28_day > 0, round(cm.INVESTMENT_DOLLARS / cw.total_walkthroughs_28_day,2), NULL) as CPWT_28_day
from campaign_insight_metrics_ cm
  inner join zenprod.ads.INSIGHTS ia on (
    cm.AD_ACCOUNT_ID = ia.AD_ACCOUNT_ID
    and cm.CAMPAIGN_ID = ia.CAMPAIGN_ID
    and ia.BREAKDOWN_TYPE = 'NONE'
    and ia.INSIGHT_TYPE = 'AGGREGATE'
  )
  inner join zenprod.ads.INSIGHTS iu on (
    cm.AD_ACCOUNT_ID = iu.AD_ACCOUNT_ID
    and cm.CAMPAIGN_ID = iu.CAMPAIGN_ID
    and iu.BREAKDOWN_TYPE = 'NONE'
    and iu.INSIGHT_TYPE = 'UNIQUE'
  )
  left join campaign_walkthroughs_ cw on (
    cm.AD_ACCOUNT_ID = cw.AD_ACCOUNT_ID
    and cm.CAMPAIGN_ID = cw.CAMPAIGN_ID
  )
;
