CREATE OR REPLACE VIEW ZENPROD.ADS.LIVERAMP_CAMPAIGN_INSIGHT_METRICS COMMENT='IN DEVELOPMENT 2020-09-22 - liveramp campaign reporting view for campaign level metrics such as impressions, clicks & spend reported daily'
AS

with lr_campaign_insight_metrics as (
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
    , insight_type
    , date
    , max_date_with_spend
    , margin
    , current_timestamp() as updated_at
    , sum(impressions) as impressions
    , sum(clicks) as clicks
    , sum(platform_spend) as platform_spend
    , sum(investment_dollars) as investment_dollars
  from zenprod.ads.liveramp_ad_insight_metrics
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)

select * from lr_campaign_insight_metrics;
