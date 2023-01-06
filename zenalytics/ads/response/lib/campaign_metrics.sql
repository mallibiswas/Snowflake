---------------------------------------------------------------
-------------------------- CAMPAIGN METRICS -------------------
---------------------------------------------------------------

use database zenalytics;
use warehouse &{whname};
use schema &{schemaname};
use role &{rolename};

--- get insights for campaign based on BID
create or replace transient table  zenalytics.ads_campaigns.campaign_metrics
AS
WITH
adsbiz_ as (
--  adsbiz, bids, camaign start stop
  select
    business_id
    , case when parent_id = '' then NULL else parent_id end as parent_id
    , name
    , fb_ad_acct_id as ad_account_id
    , row_number() over (partition by fb_ad_acct_id order by parent_id desc) as fb_ad_acct_rank
  from zenalytics.ads_campaigns.adsbiz
  where fb_ad_acct_id <> ''
   )
, campaigns_ as
  (
  -- get campaign start stop time
  select campaign_id,
         c.name,
         a.ad_account_id,
         start_time as campaign_begin_datetime,
         stop_time as campaign_end_datetime
  from zenalytics.ads_campaigns.campaigns c, adsbiz_ a
  where c.ad_account_id = a.ad_account_id
)
, insights_ as
 (
    select
        campaign_id
        , insight_id
        , insight_type
        , breakdown_type
        , try_to_date(breakdown_value) as date
        , breakdown_value
        , impressions
        , clicks
        , walkthroughs
        , engagement
        , spend_cents
        , last_synced
        , row_number() over (partition by insight_id order by last_synced desc) as latest_sync_of_the_day
   from zenalytics.ads_campaigns.insights i
   where breakdown_type = 'DAY'
 )
, traffic_ as (
--  sum of walkins over ad account (parent)
    select
        business_id,
        to_date(date_hour) as date,
        sum(walkin_merchant) as walkin_merchant,
        sum(walkin_network) as walkin_network,
        sum(walkin_unidentified) as walkin_unidentified,
        sum(walkby_merchant) as walkby_merchant,
        sum(walkby_network) as walkby_network,
        sum(walkby_unidentified) as walkby_unidentified,
        (sum(walkin_merchant) + sum(walkin_network)) as identified_walkins,
        (sum(walkin_merchant) + sum(walkin_network) + sum(walkin_unidentified)) as total_walkins
    from zenalytics.ads_campaigns.presence_sampling_stats p
    group by 1, 2
  )
, sampling_ as (
-- new 30 day rolling Recognition Rate
  select
    business_id
    , day as date
    , sample_rate_multiplier
  from zenalytics.ads_campaigns.sample_rates
)
, campaign_goals_ as (
  select
    zcr.campaign_id
  , zc.zenreach_campaign_id
  , zrcl.location_id
  , zcr.platform
  , name
  , campaign_goal
  , creation_source
  from zenalytics.ads_campaigns.zenreach_campaigns zc
  left join zenalytics.ads_campaigns.zenreach_campaign_records zcr
      on zc.zenreach_campaign_id = zcr.zenreach_campaign_id
  left join zenalytics.ads_campaigns.zenreach_campaign_records_locations zrcl
      on zcr.zenreach_campaign_records_id = zrcl.zenreach_campaign_records_id
  where zc.status <> 'FAILED'
)
, margins_ as (
  select
    *
    , lag(updated) over (partition by campaign_id order by updated desc) as effective_through
  from zenalytics.ads_campaigns.zenreach_campaign_records_margins m
)
select distinct
  coalesce(a.parent_id, a.business_id) as parent_id
  , a.business_id
  , a.ad_account_id
  , coalesce(b.parent_name, b.business_name, bp.name) as parent_name
  , coalesce(b.business_name, bp.name) as business_name
  , c.campaign_id
  , c.name
  , c.campaign_begin_datetime
  , c.campaign_end_datetime
  , i.insight_id
  , i.insight_type
  , i.breakdown_type
  , try_to_date(i.breakdown_value) as date
  , i.breakdown_value
  , i.impressions
  , i.clicks
  , i.walkthroughs
  , i.walkthroughs as confirmed_walkthroughs
  , i.engagement
  , i.spend_cents
  , m.margin_percent
  , (i.spend_cents / 100) / (1 - m.margin_percent) as investment_dollars
  , i.last_synced
  , t.walkin_merchant
  , t.walkin_network
  , t.walkin_unidentified
  , t.walkby_merchant
  , t.walkby_network
  , t.walkby_unidentified
  , 1/s.sample_rate_multiplier as sample_rate
  , s.sample_rate_multiplier
  , case when g.campaign_goal = 'CUSTOMER_ACQUISITION' then round(i.walkthroughs * s.sample_rate_multiplier)
         when g.campaign_goal = 'LOYALTY' then round(i.walkthroughs * 1) -- no sampling required
         when g.campaign_goal = 'LOST_CUSTOMER' then round(i.walkthroughs * 1) -- no sampling required
         when g.campaign_goal is null then round(i.walkthroughs * s.sample_rate_multiplier) -- if unknown, default to sampling
         else round(i.walkthroughs * s.sample_rate_multiplier)
         end as sampled_walkthroughs
  , g.campaign_goal
  , g.creation_source
  , current_timestamp() as asof_date
from adsbiz_ a
join campaigns_ c on a.ad_account_id = c.ad_account_id
join zenalytics.ads_campaigns.ad_accounts aa on a.ad_account_id = aa.ad_account_id
join insights_ i on c.campaign_id = i.campaign_id
left join traffic_ t on a.business_id = t.business_id and i.date = t.date
left join sampling_ s on a.business_id = s.business_id and i.date = s.date
left join campaign_goals_ g on c.campaign_id = g.campaign_id
left join  margins_ m
          on c.campaign_id = m.campaign_id
          and try_to_date(i.breakdown_value) >= updated
          and try_to_date(i.breakdown_value) < ifnull(effective_through, current_date())
left join zenalytics.crm.businessprofile_hierarchy b on a.business_id = b.business_id
left join zenalytics.crm.portal_businessprofile bp on a.business_id = bp.business_id
where
    ( a.fb_ad_acct_rank = 1 or a.parent_id <> '' ) -- logic to handle cases in which a single ad_account_id is attached to both parent and child locations so only one is included in campaign_metrics
    and aa.is_zenreach = TRUE
    and i.latest_sync_of_the_day = 1
    and try_to_date(i.breakdown_value) < current_date();
