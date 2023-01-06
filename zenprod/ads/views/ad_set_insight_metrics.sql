CREATE OR REPLACE VIEW ZENPROD.ADS.AD_SET_INSIGHT_METRICS COMMENT='IN DEVELOPMENT 2020-07-16 - unified ads reporting view for ad-level metrics such as impressions, clicks & spend reported daily'
AS

with facebook_ad_set_insight_metrics_ as (
    select
        parent_id
        , parent_name
        , ad_account_id
        , campaign_id
        , campaign_name
        , campaign_goal
        , ad_set_id
        , ad_set_name
        , insight_type
        , date
        , impressions
        , clicks
        , link_clicks
        , engagements
        , margin
        , platform_spend
        , investment_dollars
        , insight_id
        , max_date_with_spend
        , updated_at
    from zenprod.ads.facebook_ad_set_insight_metrics
)

, liveramp_ad_set_insight_metrics_ as (
    select
        parent_id
        , parent_name
        , ad_account_id
        , campaign_id
        , campaign_name
        , campaign_goal
        , ad_group_id as ad_set_id
        , ad_group_name as ad_set_name
        , insight_type
        , date
        , impressions
        , clicks
        , clicks as link_clicks
        , 0 as engagements
        , margin
        , platform_spend
        , investment_dollars
        , NULL as insight_id
        , max_date_with_spend
        , updated_at
    from zenprod.ads.liveramp_ad_set_insight_metrics
)

select * from facebook_ad_set_insight_metrics_ union all select * from liveramp_ad_set_insight_metrics_;
