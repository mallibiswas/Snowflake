CREATE OR REPLACE VIEW ZENPROD.ADS.CAMPAIGN_WALKTHROUGHS COMMENT='IN DEVELOPMENT 2020-07-16 - unified ads reporting view for ad-level metrics such as impressions, clicks & spend reported daily'
AS

with facebook_campaign_walkthroughs_ as (
    select
        ad_account_id
        , campaign_id
        , campaign_name
        , campaign_goal
        , merchant_dashboard_attribution_window
        , business_id
        , insight_type
        , date
        , confirmed_walkthroughs
        , sample_rate_multiplier
        , total_walkthroughs
        , confirmed_walkthroughs_1_day
        , total_walkthroughs_1_day
        , confirmed_walkthroughs_7_day
        , total_walkthroughs_7_day
        , 0 as confirmed_walkthroughs_14_day
        , 0 as total_walkthroughs_14_day
        , confirmed_walkthroughs_28_day
        , total_walkthroughs_28_day
        , campaign_data_source
    from zenprod.ads.facebook_campaign_walkthroughs
)

, liveramp_campaign_walkthroughs_ as (
    select
        ad_account_id
        , campaign_id
        , campaign_name
        , campaign_goal
        , '7' as merchant_dashboard_attribution_window
        , location_id as business_id
        , insight_type
        , date
        , confirmed_walkthroughs
        , sample_rate_multiplier
        , total_walkthroughs
        , 0 as confirmed_walkthroughs_1_day
        , 0 as total_walkthroughs_1_day
        , confirmed_walkthroughs_7_day
        , total_walkthroughs_7_day
        , confirmed_walkthroughs_14_day
        , total_walkthroughs_14_day
        , confirmed_walkthroughs_28_day
        , total_walkthroughs_28_day
        , 'LIVERAMP' as campaign_data_source
    from zenprod.ads.liveramp_campaign_walkthroughs
)

select * from facebook_campaign_walkthroughs_ union all select * from liveramp_campaign_walkthroughs_;
