CREATE OR REPLACE VIEW ZENPROD.ADS.AD_SET_WALKTHROUGHS COMMENT='IN DEVELOPMENT 2020-07-16 - unified ads reporting view for ad-level metrics such as impressions, clicks & spend reported daily'
AS

with facebook_ad_set_walkthroughs_ as (
    select
        ad_account_id
        , campaign_id
        , ad_set_id
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
    from zenprod.ads.facebook_ad_set_walkthroughs
)

, liveramp_ad_set_walkthroughs_ as (
    select
        ad_account_id
        , campaign_id
        , ad_group_id as ad_set_id
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
    from zenprod.ads.liveramp_ad_set_walkthroughs
)

select * from facebook_ad_set_walkthroughs_ union all select * from liveramp_ad_set_walkthroughs_;
