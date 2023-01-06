CREATE OR REPLACE VIEW ZENPROD.ADS.AD_SET_INSIGHT_METRICS_NEW COMMENT='Combined FB and TTD Insight Metrics from Cache'
AS

with enriched_zenreach_campaigns_with_dates_ as (
    select distinct
        parent_id,
        parent_name,
        ad_account_id,
        platform,
        zenreach_campaign_id,
        campaign_name,
        is_ad_account_campaign,
        number_of_locations,
        zenreach_campaign_records_id,
        platform_campaign_id,
        date,
        margin
    from zenprod.ads.campaign_insight_metrics_new
)

, zenreach_ad_sets_combined_ as (
    select
        campaign_id,
        ad_set_id,
        name as ad_set_name
    from zenprod.ads.ad_sets
    union
    select
        lc.campaign_id,
        lag.ad_group_id as ad_set_id,
        lag.name as ad_set_name
    from zenprod.ads.lr_ad_groups lag
    inner join zenprod.ads.lr_campaigns lc
        on lag.campaign_id = lc.id
)

, zenreach_ad_sets_combined_metrics_ as (
    select
        insights_ad_sets_id,
        impressions,
        clicks,
        engagement,
        thru_plays,
        video_plays,
        video_plays_at25_perc,
        video_plays_at50_perc,
        video_plays_at75_perc,
        video_plays_at95_perc,
        video_plays_at100_perc,
        video_average_play_time,
        link_clicks,
        outbound_clicks,
        e_cpm_cents,
        null as sampled_tracked_impressions,
        null as sampled_viewed_impressions
    from insights_ad_sets_fb_metrics
    union
    select
        insights_ad_sets_id,
        impressions,
        clicks,
        null as engagement,
        null as thru_plays,
        player_starts as video_plays,
        player25perc_complete as video_plays_at25_perc,
        player50perc_complete as video_plays_at50_perc,
        player75perc_complete as video_plays_at75_perc,
        null as video_plays_at95_perc,
        player_completed_views as video_plays_at100_perc,
        null as video_average_play_time,
        null as link_clicks,
        null as outbound_clicks,
        null as e_cpm_cents,
        sampled_tracked_impressions,
        sampled_viewed_impressions   
    from insights_ad_sets_ttd_metrics
)

, zenreach_ad_set_insights_ as (
    select
        ezc.parent_id,
        ezc.parent_name,
        ezc.ad_account_id,
        ezc.platform,
        ias.zenreach_campaign_id,
        ezc.campaign_name,
        ezc.is_ad_account_campaign,
        ezc.number_of_locations,
        ezc.zenreach_campaign_records_id,
        ias.platform_campaign_id,
        ias.ad_set_id,
        zasc.ad_set_name,
        ias.insight_type,
        ias.breakdown_type,
        try_to_date(ias.breakdown_value) as date,
        zascm.impressions,
        zascm.clicks,
        zascm.link_clicks,
        zascm.engagement as engagements,
        zascm.thru_plays,
        zascm.video_plays,
        zascm.video_plays_at25_perc,
        zascm.video_plays_at50_perc,
        zascm.video_plays_at75_perc,
        zascm.video_plays_at95_perc,
        zascm.video_plays_at100_perc,
        zascm.video_average_play_time,
        zascm.outbound_clicks,
        ezc.margin,
        zascm.e_cpm_cents,
        zascm.sampled_tracked_impressions,
        zascm.sampled_viewed_impressions,
        round(ias.spend_cents / 100, 2) as platform_spend,
        round(ias.billable_cents / 100, 2) as investment_dollars,
        ias.id as insight_id,
        ias.updated as updated_at
    from zenprod.ads.insights_ad_sets ias
    inner join zenreach_ad_sets_combined_metrics_ zascm
        on ias.id = zascm.insights_ad_sets_id
    inner join enriched_zenreach_campaigns_with_dates_ ezc
        on ias.zenreach_campaign_id = ezc.zenreach_campaign_id
        and ias.platform_campaign_id = ezc.platform_campaign_id
        and ias.breakdown_value = to_char(ezc.date)
    inner join zenreach_ad_sets_combined_ zasc
        on ias.platform_campaign_id = zasc.campaign_id
        and ias.ad_set_id = zasc.ad_set_id
    where ias.breakdown_type = 'DAY'
)

select * from zenreach_ad_set_insights_;
