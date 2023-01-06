CREATE OR REPLACE VIEW ZENPROD.ADS.LIVERAMP_AD_WALKTHROUGHS COMMENT='IN DEVELOPMENT 2020-09-22 - liveramp ads reporting view for ad-level metrics such as impressions, clicks & spend reported daily'
AS

with liveramp_walkthroughs_ as (
    select
        aim.parent_id
        , aim.parent_name
        , aim.ad_account_id
        , aim.zenreach_campaign_id
        , aim.zenreach_campaign_records_id
        , aim.campaign_uuid
        , aim.campaign_id
        , aim.campaign_name
        , aim.campaign_goal
        , aim.ad_group_uuid
        , aim.ad_group_id
        , aim.ad_group_name
        , aim.ad_group_goal
        , aim.creative_uuid
        , aim.creative_id
        , aim.creative_name
        , aim.insight_type
        , aim.date
        , zcrl.location_id
        , ifnull(s.sample_rate_multiplier,0) as sample_rate_multiplier
        , round(ifnull(liw.aggregate7day,0)) as confirmed_walkthroughs
        , case when aim.ad_group_goal = 'CUSTOMER_ACQUISITION' then round(confirmed_walkthroughs * ifnull(s.sample_rate_multiplier,0))
            when aim.ad_group_goal = 'LOYALTY' then round(confirmed_walkthroughs * 1) -- no sampling required
            when aim.ad_group_goal = 'LOST_CUSTOMER' then round(confirmed_walkthroughs * 1) -- no sampling required
            when aim.ad_group_goal is null then round(confirmed_walkthroughs * ifnull(s.sample_rate_multiplier,0)) -- if unknown, default to sampling
            else round(confirmed_walkthroughs * ifnull(s.sample_rate_multiplier,0))
          end as total_walkthroughs
        , round(ifnull(liw.aggregate7day,0)) as confirmed_walkthroughs_7_day
        , case when aim.ad_group_goal = 'CUSTOMER_ACQUISITION' then round(confirmed_walkthroughs_7_day * ifnull(s.sample_rate_multiplier,0))
            when aim.ad_group_goal = 'LOYALTY' then round(confirmed_walkthroughs_7_day * 1) -- no sampling required
            when aim.ad_group_goal = 'LOST_CUSTOMER' then round(confirmed_walkthroughs_7_day * 1) -- no sampling required
            when aim.ad_group_goal is null then round(confirmed_walkthroughs_7_day * ifnull(s.sample_rate_multiplier,0)) -- if unknown, default to sampling
            else round(confirmed_walkthroughs_7_day * ifnull(s.sample_rate_multiplier,0))
          end as total_walkthroughs_7_day
        , round(ifnull(liw.aggregate14day,0)) as confirmed_walkthroughs_14_day
        , case when aim.ad_group_goal = 'CUSTOMER_ACQUISITION' then round(confirmed_walkthroughs_14_day * ifnull(s.sample_rate_multiplier,0))
            when aim.ad_group_goal = 'LOYALTY' then round(confirmed_walkthroughs_14_day * 1) -- no sampling required
            when aim.ad_group_goal = 'LOST_CUSTOMER' then round(confirmed_walkthroughs_14_day * 1) -- no sampling required
            when aim.ad_group_goal is null then round(confirmed_walkthroughs_14_day * ifnull(s.sample_rate_multiplier,0)) -- if unknown, default to sampling
            else round(confirmed_walkthroughs_14_day * ifnull(s.sample_rate_multiplier,0))
          end as total_walkthroughs_14_day
        , round(ifnull(liw.aggregate28day,0)) as confirmed_walkthroughs_28_day
        , case when aim.ad_group_goal = 'CUSTOMER_ACQUISITION' then round(confirmed_walkthroughs_28_day * ifnull(s.sample_rate_multiplier,0))
            when aim.ad_group_goal = 'LOYALTY' then round(confirmed_walkthroughs_28_day * 1) -- no sampling required
            when aim.ad_group_goal = 'LOST_CUSTOMER' then round(confirmed_walkthroughs_28_day * 1) -- no sampling required
            when aim.ad_group_goal is null then round(confirmed_walkthroughs_28_day * ifnull(s.sample_rate_multiplier,0)) -- if unknown, default to sampling
            else round(confirmed_walkthroughs_28_day * ifnull(s.sample_rate_multiplier,0))
          end as total_walkthroughs_28_day
    from zenprod.ads.liveramp_ad_insight_metrics aim
    left join zenprod.ads.zenreach_campaign_records_locations zcrl
        on aim.zenreach_campaign_records_id = zcrl.zenreach_campaign_records_id
    left join zenprod.ads.sample_rates s
        on zcrl.location_id = s.business_id
        and aim.date = s.day
    left join zenprod.ads.lr_impressions_walkthroughs_by_day liw
        on aim.creative_uuid = liw.creative_id
        and aim.date = liw.sighting_day
        and zcrl.location_id = liw.location_id
)

select * from liveramp_walkthroughs_;
