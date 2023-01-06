CREATE OR REPLACE VIEW AD_SET_SUMMARY COMMENT='IN DEVELOPMENT 2020-07-16 - unified ads reporting view for ad-level metrics such as impressions, clicks & spend reported daily'
AS

with facebook_ad_set_summary_ as (
    select
        parent_id
        , parent_name
        , ad_account_id
        , campaign_id
        , campaign_name
        , campaign_goal
        , impressions
        , reach
        , frequency
        , clicks
        , link_clicks
        , engagements
        , investment_dollars
        , platform_spend
        , confirmed_walkthroughs
        , total_walkthroughs
        , cpwt
        , ctr
        , cpm
        , confirmed_walkthroughs_1_day
        , total_walkthroughs_1_day
        , cpwt_1_day
        , confirmed_walkthroughs_7_day
        , total_walkthroughs_7_day
        , cpwt_7_day
        , 0 as confirmed_walkthroughs_14_day
        , 0 as total_walkthroughs_14_day
        , NULL as cpwt_14_day
        , confirmed_walkthroughs_28_day
        , total_walkthroughs_28_day
        , cpwt_28_day
    from zenprod.ads.facebook_ad_set_summary
)

, liveramp_ad_set_summary_ as (
    select
        parent_id
        , parent_name
        , ad_account_id
        , campaign_id
        , campaign_name
        , campaign_goal
        , impressions
        , 0 as reach
        , 0 as frequency
        , clicks
        , link_clicks
        , 0 as engagements
        , investment_dollars
        , platform_spend
        , confirmed_walkthroughs
        , total_walkthroughs
        , cpwt
        , ctr
        , cpm
        , 0 as confirmed_walkthroughs_1_day
        , 0 as total_walkthroughs_1_day
        , NULL as cpwt_1_day
        , confirmed_walkthroughs_7_day
        , total_walkthroughs_7_day
        , cpwt_7_day
        , confirmed_walkthroughs_14_day
        , total_walkthroughs_14_day
        , cpwt_14_day
        , confirmed_walkthroughs_28_day
        , total_walkthroughs_28_day
        , cpwt_28_day
    from zenprod.ads.liveramp_campaign_summary
)

select * from facebook_ad_set_summary_ union all select * from liveramp_ad_set_summary_;
