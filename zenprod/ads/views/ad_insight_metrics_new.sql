CREATE OR REPLACE VIEW ZENPROD.ADS.AD_INSIGHT_METRICS_NEW COMMENT='Combined FB and TTD Insight Metrics from Cache'
AS

with enriched_zenreach_ad_sets_with_dates_ as (
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
        ad_set_id,
        ad_set_name,
        date,
        margin
    from zenprod.ads.ad_set_insight_metrics_new
)

, zenreach_ads_combined_ as (
    select
        campaign_id,
        ad_set_id,
        ad_id,
        name as ad_name
    from zenprod.ads.ads
    union
    select
        lc.campaign_id,
        lag.ad_group_id as ad_set_id,
        lcr.creative_id as ad_id,
        lcr.name as ad_name
    from zenprod.ads.lr_creatives lcr
    inner join zenprod.ads.lr_ad_groups lag
        on lcr.ad_group_id = lag.id
    inner join zenprod.ads.lr_campaigns lc
        on lag.campaign_id = lc.id
)

, zenreach_ads_combined_metrics_ as (
    select
        insights_ads_id,
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
    from insights_ads_fb_metrics
    union
    select
        insights_ads_id,
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
    from insights_ads_ttd_metrics
)

, zenreach_ad_insights_ as (
    select
        ezas.parent_id,
        ezas.parent_name,
        ezas.ad_account_id,
        ezas.platform,
        ia.zenreach_campaign_id,
        ezas.campaign_name,
        ezas.is_ad_account_campaign,
        ezas.number_of_locations,
        ezas.zenreach_campaign_records_id,
        ia.platform_campaign_id,
        ezas.ad_set_id,
        ezas.ad_set_name,
        ia.ad_id,
        zac.ad_name,
        ia.insight_type,
        ia.breakdown_type,
        try_to_date(ia.breakdown_value) as date,
        zacm.impressions,
        zacm.clicks,
        zacm.link_clicks,
        zacm.engagement as engagements,
        zacm.thru_plays,
        zacm.video_plays,
        zacm.video_plays_at25_perc,
        zacm.video_plays_at50_perc,
        zacm.video_plays_at75_perc,
        zacm.video_plays_at95_perc,
        zacm.video_plays_at100_perc,
        zacm.video_average_play_time,
        zacm.outbound_clicks,
        ezas.margin,
        zacm.e_cpm_cents,
        zacm.sampled_tracked_impressions,
        zacm.sampled_viewed_impressions,
        round(ia.spend_cents / 100, 2) as platform_spend,
        round(ia.billable_cents / 100, 2) as investment_dollars,
        ia.id as insight_id,
        ia.updated as updated_at
    from zenprod.ads.insights_ads ia
    inner join zenreach_ads_combined_metrics_ zacm
        on ia.id = zacm.insights_ads_id
    inner join enriched_zenreach_ad_sets_with_dates_ ezas
        on ia.zenreach_campaign_id = ezas.zenreach_campaign_id
        and ia.platform_campaign_id = ezas.platform_campaign_id
        and ia.ad_set_id = ezas.ad_set_id
        and ia.breakdown_value = to_char(ezas.date)
    inner join zenreach_ads_combined_ zac
        on ia.platform_campaign_id = zac.campaign_id
        and ia.ad_set_id = zac.ad_set_id
        and ia.ad_id = zac.ad_id
    where ia.breakdown_type = 'DAY'
)

select * from zenreach_ad_insights_;
