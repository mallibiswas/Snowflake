CREATE OR REPLACE VIEW LIVERAMP_AD_SET_WALKTHROUGHS COMMENT='IN DEVELOPMENT 2020-09-22 - liveramp ad set reporting view for ad set walkthroughs by locations reported daily'
AS

with lr_ad_set_walkthroughs as (
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
    , ad_group_uuid
    , ad_group_id
    , ad_group_name
    , ad_group_goal
    , insight_type
    , date
    , location_id
    , sample_rate_multiplier
    , sum(confirmed_walkthroughs) as confirmed_walkthroughs
    , sum(total_walkthroughs) as total_walkthroughs
    , sum(confirmed_walkthroughs_7_day) as confirmed_walkthroughs_7_day
    , sum(total_walkthroughs_7_day) as total_walkthroughs_7_day
    , sum(confirmed_walkthroughs_14_day) as confirmed_walkthroughs_14_day
    , sum(total_walkthroughs_14_day) as total_walkthroughs_14_day
    , sum(confirmed_walkthroughs_28_day) as confirmed_walkthroughs_28_day
    , sum(total_walkthroughs_28_day) as total_walkthroughs_28_day
  from zenprod.ads.liveramp_ad_walkthroughs
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)

select * from lr_ad_set_walkthroughs;
