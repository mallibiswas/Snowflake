CREATE OR REPLACE VIEW ZENPROD.ADS.CAMPAIGN_SUMMARY COMMENT='IN DEVELOPMENT 2020-07-16 - unified ads reporting view for ad-level metrics such as impressions, clicks & spend reported daily'
AS

with facebook_campaign_summary_ as (
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
        , days_live
        , min_date
        , max_date
        , active
        , missing_margins
        , spend_missing_margins
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
    from zenprod.ads.facebook_campaign_summary
)

, liveramp_campaign_summary_ as (
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
        , days_live
        , min_date
        , max_date
        , active
        , missing_margins
        , spend_missing_margins
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

select * from facebook_campaign_summary_ union all select * from liveramp_campaign_summary_;
