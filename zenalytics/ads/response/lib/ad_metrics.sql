---------------------------------------------------------------
-------------------------- AD METRICS -------------------
---------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};
use role &{rolename};

-- Create Ad-level metrics for reporting
-- NOTES: Traffic (walkins / walkbys) is being constructed from raw wifi_consented_sightings which (a) will
-- ultimately become really inefficient, but for now is running in about 30 seconds and (b) doesn't cover
-- the full history of all campaigns. But I believe we are only using this in minor side-reporting for recent campaigns.
-- NOTES: A small number of campaigns have been run with mixed audience types at the ad_set level. The objective
-- for this campaign has been set to ACQUISITION_AND_LOYALTY. It is uncertain what the specific objective type was at the ad
-- level for mixed campaigns. To reduce unintentional reporting errors these ads the sampled_walkthroughs = NULL, they
-- will need to be calculated manually. However, the sampling
-- NOTES: There are additional video play metrics available that I didn't pull in to keep this table somewhat minimal


create or replace table zenalytics.ads_campaigns.ad_metrics
as
with traffic_ as (
select
  to_date(end_time) as end_date
  , location_id
  , sum(case when contact_id is not null and known_to_merchant_account = TRUE then 1 else 0 end) as merchant_walkins
  , sum(case when contact_id is not null and known_to_merchant_account = FALSE then 1 else 0 end) as network_walkins
  , sum(case when contact_id is not null then 1 else 0 end) as identified_walkins
  , sum(case when contact_id is null then 1 else 0 end) as unidentified_walkins
  , count(*) as all_walkins
from zenprod.presence.wifi_consented_sightings
where classification = 'Classification_WALKIN'
group by 1,2)
, margins_ as (
  select
    *
    , lag(updated) over (partition by campaign_id order by updated desc) as effective_through
  from zenalytics.ads_campaigns.zenreach_campaign_records_margins m
)
, ad_metrics_ as (
select
    coalesce(bh.parent_id, zcrl.location_id) as parent_id
    , coalesce(bh.parent_name, bf.name) as parent_name
    , zcrl.location_id
    , coalesce(bh.business_name, bf.name) as business_name
    , i.ad_account_id
    , i.campaign_id
    , c.name as campaign_name
    , c.start_time as campaign_begin_datetime
    , c.stop_time as campaign_end_datetime
    , zc.campaign_goal
    , i.ad_set_id
    , a_s.name as ad_set_name
    , i.ad_id
    , a.name as ad_name
    , i.insight_id
    , i.insight_type
    , try_to_date(i.breakdown_value) as insight_date
    , i.impressions
    , i.clicks
    , i.engagement
    , t.merchant_walkins
    , t.network_walkins
    , t.identified_walkins
    , t.unidentified_walkins
    , t.all_walkins
    , i.walkthroughs as confirmed_walkthroughs
    , 1/s.sample_rate_multiplier as sample_rate
    , s.sample_rate_multiplier
    , case
         -- support for mixed objectives at the ad_set level campaigns
         when zc.campaign_goal = 'ACQUISITION_AND_LOYALTY' and a_s.name like 'LAL_%' then round(i.walkthroughs * s.sample_rate_multiplier) -- if unknown, default to sampling
         when zc.campaign_goal = 'ACQUISITION_AND_LOYALTY' and a_s.name like 'RTG_%' then round(i.walkthroughs * 1) -- if unknown, default to sampling
         when c.name like 'MIX_%' and a_s.name like 'LAL_%' then round(i.walkthroughs * s.sample_rate_multiplier)
         when c.name like 'MIX_%' and a_s.name like 'RTG_%' then round(i.walkthroughs * 1)
         when zc.campaign_goal = 'CUSTOMER_ACQUISITION' then round(i.walkthroughs * s.sample_rate_multiplier)
         when zc.campaign_goal = 'LOYALTY' then round(i.walkthroughs * 1) -- no sampling required
         when zc.campaign_goal = 'LOST_CUSTOMER' then round(i.walkthroughs * 1) -- no sampling required
         else round(i.walkthroughs * s.sample_rate_multiplier)
         end as sampled_walkthroughs
      , i.video_plays
      , i.link_clicks
      , i.spend_cents
      , m.margin_percent
      , (i.spend_cents / 100) / (1 - m.margin_percent) as investment_dollars
      , i.e_cpm_cents
      , least(i.last_synced
             , ac.last_synced
             , c.last_synced
             , zcr.asof_date
             , zcrl.asof_date
             , zc.asof_date
             , s.asof_date
             ) as last_synced
      , current_timestamp() as asof_date
      -- if we unintentionally get data for the same ad and insight type (unique / aggregate) on the same day twice, pick one
      , row_number() over (partition by i.ad_id, try_to_date(i.breakdown_value), insight_type
                       order by random(1)) as insight_rank
from zenalytics.ads_campaigns.ad_insights i
join zenalytics.ads_campaigns.ads a
    on i.ad_id = a.ad_id
join zenalytics.ads_campaigns.ad_accounts ac
  on i.ad_account_id = ac.ad_account_id
left join zenalytics.ads_campaigns.ad_sets a_s
  on i.ad_set_id = a_s.ad_set_id
left join zenalytics.ads_campaigns.campaigns c
  on i.campaign_id = c.campaign_id
left join zenalytics.ads_campaigns.zenreach_campaign_records zcr
  on i.campaign_id = zcr.campaign_id
left join zenalytics.ads_campaigns.zenreach_campaigns zc
  on zcr.zenreach_campaign_id = zc.zenreach_campaign_id
left join zenalytics.ads_campaigns.zenreach_campaign_records_locations zcrl
  on zcr.zenreach_campaign_records_id = zcrl.zenreach_campaign_records_id
left join margins_ m
  on c.campaign_id = m.campaign_id
    and try_to_date(i.breakdown_value) >= m.updated
    and try_to_date(i.breakdown_value) < ifnull(m.effective_through, current_date())
left join traffic_ t
  on zcrl.location_id = t.location_id
     and try_to_date(i.breakdown_value) = t.end_date
left join zenalytics.ads_campaigns.sample_rates s
  on zcrl.location_id = s.business_id
     and try_to_date(i.breakdown_value) = s.day
left join zenalytics.crm.businessprofile_hierarchy bh
  on zcrl.location_id = bh.business_id
left join zenalytics.crm.portal_businessprofile bf
  on zcrl.location_id = bf.business_id
where
    ac.is_zenreach = TRUE
    and zcrl.location_id is not null)
select *
from ad_metrics_
where insight_rank = 1
      and insight_date < current_date()
order by insight_date desc;
