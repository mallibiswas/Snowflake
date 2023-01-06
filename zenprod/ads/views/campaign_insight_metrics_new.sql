CREATE OR REPLACE VIEW ZENPROD.ADS.CAMPAIGN_INSIGHT_METRICS_NEW COMMENT='Combined FB and TTD Insight Metrics from Cache'
AS

with zenreach_ad_account_campaigns_ as (
    select
        zadc.zenreach_campaign_id,
        zad.account_id parent_id,
        zad.platform,
        coalesce(zafb.facebook_ad_account_id, zad.ad_account_id) as ad_account_id
    from ads.zenreach_ad_account_campaigns zadc 
    join ads.zenreach_ad_accounts zad
        on zadc.ad_account_id = zad.ad_account_id
    left join ads.zenreach_ad_account_fb_config zafb
        on zad.ad_account_id = zafb.ad_account_id
)

, zenreach_campaigns_ as (
    select
        zc.zenreach_campaign_id,
        zaac.parent_id is not null as is_ad_account_campaign,
        zc.name as campaign_name,
        zc.campaign_goal as campaign_goal,
        zcr.zenreach_campaign_records_id,
        zcr.campaign_id,
        zcr.platform,
        coalesce(zcr.platform_account_id, zaac.ad_account_id) as ad_account_id,
        zcrl.location_id as location_id,
        coalesce(zaac.parent_id, zcrl.location_id) as business_id
    from ads.zenreach_campaigns zc
    inner join ads.zenreach_campaign_records zcr
        on zc.zenreach_campaign_id = zcr.zenreach_campaign_id
    left join ads.zenreach_campaign_records_locations zcrl
        on zcr.zenreach_campaign_records_id = zcrl.zenreach_campaign_records_id
    left join zenreach_ad_account_campaigns_ zaac
        on zc.zenreach_campaign_id = zaac.zenreach_campaign_id
)

, margins_ as (
  select
    *
    , lag(updated) over (partition by ZENREACH_CAMPAIGN_RECORDS_ID order by updated desc) as effective_through
  from zenprod.ads.zenreach_campaign_records_margins m
)

, zenreach_campaign_record_counts_ as (
    select
        zenreach_campaign_id,
        count(*) as number_of_campaign_records
    from ads.zenreach_campaign_records
    group by 1
)

, zenreach_campaign_location_counts_ as (
    select
        zenreach_campaign_id,
        count(location_id) as number_of_locations
    from zenreach_campaigns_
    group by 1
)

, enriched_zenreach_campaigns_ as (
    select distinct
        coalesce(bh.parent_id, zc.business_id) as parent_id,
        coalesce(bh.parent_name, bf.name) as parent_name,
        zc.zenreach_campaign_id,
        zc.platform,
        zc.is_ad_account_campaign,
        zc.ad_account_id,
        zc.zenreach_campaign_records_id,
        zc.campaign_id,
        zc.campaign_name,
        zc.campaign_goal,
        zclc.number_of_locations
    from zenreach_campaigns_ zc
    left join zenprod.crm.businessprofile_hierarchy bh
        on zc.business_id = bh.business_id
    left join zenprod.crm.portal_businessprofile bf
        on zc.business_id = bf.business_id
    left join zenreach_campaign_record_counts_ zcrc
        on zc.zenreach_campaign_id = zcrc.zenreach_campaign_id
    left join zenreach_campaign_location_counts_ zclc
        on zc.zenreach_campaign_id = zclc.zenreach_campaign_id    
)

, zenreach_campaign_combined_metrics_ as (
    select
        insights_campaigns_id,
        impressions,
        clicks,
        engagement,
        thru_plays,
        video_plays,
        link_clicks,
        outbound_clicks
    from insights_campaigns_fb_metrics
    union
    select
        insights_campaigns_id,
        impressions,
        clicks,
        null as engagement,
        null as thru_plays,
        null as video_plays,
        clicks as link_clicks,
        clicks as outbound_clicks
    from insights_campaigns_ttd_metrics
)

, zenreach_campaign_insights_ as (
    select
        ezc.parent_id,
        ezc.parent_name,
        ezc.ad_account_id,
        ezc.platform,
        ic.zenreach_campaign_id,
        ezc.campaign_name,
        ezc.is_ad_account_campaign,
        ezc.number_of_locations,
        ezc.zenreach_campaign_records_id,
        ic.platform_campaign_id,
        ic.insight_type,
        ic.breakdown_type,
        try_to_date(ic.breakdown_value) as date,
        zccm.impressions,
        zccm.clicks,
        zccm.engagement,
        zccm.thru_plays,
        zccm.video_plays,
        zccm.link_clicks,
        zccm.outbound_clicks,
        m.margin_percent as margin,
        round(ic.spend_cents / 100, 2) as platform_spend,
        round(ic.billable_cents / 100, 2) as investment_dollars,
        ic.id as insight_id,
        ic.updated as updated_at
    from zenprod.ads.insights_campaigns ic
    inner join zenreach_campaign_combined_metrics_ zccm
        on ic.id = zccm.insights_campaigns_id
    inner join enriched_zenreach_campaigns_ ezc
        on ic.zenreach_campaign_id = ezc.zenreach_campaign_id
        and ic.platform_campaign_id = ezc.campaign_id
    left join margins_ m
        on ezc.zenreach_campaign_records_id = m.zenreach_campaign_records_id
        and ic.breakdown_value >= m.updated
        and ic.breakdown_value < ifnull(m.effective_through, current_date())
    where breakdown_type = 'DAY'
)

select * from zenreach_campaign_insights_;
