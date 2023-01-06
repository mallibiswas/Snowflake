CREATE OR REPLACE VIEW ZENPROD.ADS.LIVERAMP_CAMPAIGN_SUMMARY COMMENT='IN DEVELOPMENT 2020-07-16 - unified campaign reporting view for campaign-to-date summary metrics'
AS

with lr_campaign_insight_metrics_ as (
    select 
    parent_id
    , parent_name
    , ad_account_id
    , zenreach_campaign_id
    , zenreach_campaign_records_id
    , campaign_uuid
    , campaign_id
    , campaign_name
    , campaign_goal
    , sum(impressions) as impressions
    , sum(clicks) as clicks
    , sum(clicks) as link_clicks
    , sum(platform_spend) as platform_spend
    , round(sum(INVESTMENT_DOLLARS),2) as investment_dollars
    , min(date) as min_date
    , max(date) as max_date
    , count(distinct iff(platform_spend > 0, date, NULL)) as days_live
    , count(distinct iff(platform_spend > 0 and margin is null, date, NULL)) as missing_margins
    , sum(distinct iff(platform_spend > 0 and margin is null, platform_spend/100, NULL)) as spend_missing_margins
  from zenprod.ads.liveramp_campaign_insight_metrics
  where insight_type = 'AGGREGATE'
  group by 1,2,3,4,5,6,7,8,9
)

, lr_campaign_walkthroughs_ as (
    select 
    campaign_uuid
    , sum(CONFIRMED_WALKTHROUGHS) as confirmed_walkthroughs
    , sum(TOTAL_WALKTHROUGHS) as total_walkthroughs
    , sum(confirmed_walkthroughs_7_day) as confirmed_walkthroughs_7_day
    , sum(total_walkthroughs_7_day) as total_walkthroughs_7_day
    , sum(confirmed_walkthroughs_14_day) as confirmed_walkthroughs_14_day
    , sum(total_walkthroughs_14_day) as total_walkthroughs_14_day
    , sum(confirmed_walkthroughs_28_day) as confirmed_walkthroughs_28_day
    , sum(total_walkthroughs_28_day) as total_walkthroughs_28_day
  from zenprod.ads.liveramp_campaign_walkthroughs
  where insight_type = 'AGGREGATE'
  group by 1
)

select
  lcim.parent_id
  , lcim.parent_name
  , lcim.ad_account_id
  , lcim.zenreach_campaign_id
  , lcim.zenreach_campaign_records_id
  , lcim.campaign_uuid
  , lcim.campaign_id
  , lcim.campaign_name
  , lcim.campaign_goal
  , lcim.impressions
  , lcim.clicks
  , lcim.link_clicks
  , lcim.investment_dollars
  , lcim.platform_spend
  , lcm.confirmed_walkthroughs
  , lcm.total_walkthroughs
  , iff(lcm.total_walkthroughs > 0, round(lcim.investment_dollars / lcm.total_walkthroughs,2), NULL) as CPWT
  , round(iff(lcim.impressions > 0, lcim.link_clicks / lcim.impressions , NULL),4) as CTR
  , iff(lcim.impressions > 0, round(lcim.investment_dollars / (lcim.impressions / 1000) , 2), NULL) as CPM
  , lcim.min_date
  , lcim.max_date
  , case when lcim.max_date = current_date() then TRUE else FALSE end as active
  , lcim.days_live
  , lcim.missing_margins
  , lcim.spend_missing_margins
  , lcm.confirmed_walkthroughs_7_day
  , lcm.total_walkthroughs_7_day
  , iff(lcm.total_walkthroughs_7_day > 0, round(lcim.investment_dollars / lcm.total_walkthroughs_7_day,2), NULL) as CPWT_7_day
  , lcm.confirmed_walkthroughs_14_day
  , lcm.total_walkthroughs_14_day
  , iff(lcm.total_walkthroughs_14_day > 0, round(lcim.investment_dollars / lcm.total_walkthroughs_14_day,2), NULL) as CPWT_14_day
  , lcm.confirmed_walkthroughs_28_day
  , lcm.total_walkthroughs_28_day
  , iff(lcm.total_walkthroughs_28_day > 0, round(lcim.investment_dollars / lcm.total_walkthroughs_28_day,2), NULL) as CPWT_28_day
from lr_campaign_insight_metrics_ lcim
left join lr_campaign_walkthroughs_ lcm
    on lcim.campaign_uuid = lcm.campaign_uuid;
