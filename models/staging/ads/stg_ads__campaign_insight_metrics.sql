WITH facebook_campaign_insight_metrics_ as (
    select
        parent_id
        , parent_name
        , ad_account_id
        , campaign_id
        , campaign_name
        , campaign_goal
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
        , 'FB' as channel
        , updated_at
    from {{ ref('stg_ads__facebook_campaign_insight_metrics') }}
)

, liveramp_campaign_insight_metrics_ as (
    select
        parent_id
        , parent_name
        , ad_account_id
        , campaign_id
        , campaign_name
        , campaign_goal
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
        , 'LR' as channel
        , updated_at
    from {{ ref('stg_ads__liveramp_campaign_insight_metrics') }}
)
select * from facebook_campaign_insight_metrics_
union all
select * from liveramp_campaign_insight_metrics_

