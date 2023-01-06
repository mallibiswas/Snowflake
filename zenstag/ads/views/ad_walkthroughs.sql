CREATE OR REPLACE VIEW ZENSTAG.ADS.AD_WALKTHROUGHS COMMENT='IN DEVELOPMENT 2020-07-16 - unified ads reporting view for ad-level metrics such as impressions, clicks & spend reported daily'
AS
-- generate all dates back 10 years
with all_dates_ as (
  SELECT DATEADD ( DAY, '-' || ROW_NUMBER() over ( ORDER BY NULL ) , DATEADD ( DAY, '+1',
    CURRENT_DATE() ) ) AS DATE
  FROM
    TABLE ( GENERATOR ( rowcount => ( 3650 ) ) )
)

, custom_conversion_campaigns_ as (
  select 
      c.ad_account_id
      , icc.campaign_id
      , 'MULTILOC CC' as source
      , min(try_to_date(breakdown_value)) min_insight_date
      , max(try_to_date(breakdown_value)) max_insight_date
  from zenstag.ads.insights_custom_conversions icc
  left join zenstag.ads.campaigns c on icc.campaign_id = c.campaign_id
  left join zenstag.ads.zenreach_campaign_records zcr on zcr.campaign_id = c.campaign_id
  where breakdown_type = 'DAY'
  and zcr.zenreach_campaign_id in (select zenreach_campaign_id from zenstag.ads.zenreach_ad_account_campaigns)
  group by 1,2,3
)

-- calculate the first and last dates of insight_data attached to each campaign
, ad_date_range_ as (
  select
    i.ad_account_id
    , i.campaign_id
    , i.ad_set_id
    , i.ad_id
    , min(try_to_date(breakdown_value)) as min_insight_date
    , max(try_to_date(breakdown_value)) as max_insight_date
    , max(iff(spend_cents > 0, try_to_date(breakdown_value), NULL)) as max_date_with_spend
  from zenstag.ads.ad_insights i
  where breakdown_type = 'DAILY' and insight_type = 'AGGREGATE'
  group by 1,2,3,4
)

-- values of insight_type column for join
, insight_types_ as (
  select *
  from (VALUES ('AGGREGATE')
                , ('UNIQUE')) as i (insight_type) 
)
-- create all combinations of date, ad account id, campaign_id and insight type
-- columns: date, ad_account_id, campaign_id, insight, min_insight_date, max_insight_date, insight_type
, ad_dates_ as (
  select *
  from all_dates_ d
       , ad_date_range_ cdr
       , insight_types_ it
  where 
    d.date >= cdr.min_insight_date 
    and d.date <= max_insight_date
    -- ad level data isn't completely populated earlier in 2019
    -- when we started pulling this data active ads backfilled but inactive did not
    -- this can lead to confusion / inconsistencies when summing ads-level data for accounts
    -- prior to the time we started acquiring this data from FB on all live accounts (roughtly Nov 2019)
    and d.date >= '2019-11-15'
  order by campaign_id, date
)
-- the rank is used to find a single loc for a multi-loc campaign to use to walk back to the parent_id
, zenreach_campaign_records_locations_ as (
    select *, row_number() over (partition by ZENREACH_CAMPAIGN_RECORDS_ID order by LOCATION_ID) as rank
    from zenstag.ads.zenreach_campaign_records_locations
)
, campaign_goals_ as (
  select
    zcr.campaign_id
  , zc.zenreach_campaign_id
  , zcr.platform
  , zc.name
  , zc.campaign_goal
  , zc.creation_source
  , case when zcv.fb_display_attribution_window is null then '7'
    else zcv.fb_display_attribution_window
    end as fb_display_attribution_window
  from zenstag.ads.zenreach_campaigns zc
  left join zenstag.ads.zenreach_campaign_records zcr
      on zc.zenreach_campaign_id = zcr.zenreach_campaign_id
  left join zenstag.ads.zenreach_campaign_visibility zcv on
    zc.zenreach_campaign_id = zcv.zenreach_campaign_id
  where zc.status <> 'FAILED' and zcr.platform = 'FACEBOOK'
)
-- ### WALKTHROUGHS AFTER 2019-11-15 WHEN AD-LEVEL DATA BECAME RELIABLY AVAILABLE ###
-- ### CALCULATE SAMPLING BY PULLING IN AD-SET LEVEL CAMPAIGN GOAL ###
, ad_walkthroughs_after_2019_11_15_ as (
  select 
    cd.ad_account_id
    , cd.campaign_id
    , cd.ad_set_id
    , cd.ad_id
    , zcrl.location_id as business_id
    , cd.insight_type
    , cd.date
    , s.sample_rate_multiplier
    , coalesce(zasg.goal, cg.campaign_goal) as ad_set_goal
    , case when cg.fb_display_attribution_window = '1' then ifnull(i.walkthroughs1_day,0)
        when cg.fb_display_attribution_window = '7' then ifnull(i.walkthroughs7_day,0)
        when cg.fb_display_attribution_window = '28' then ifnull(i.walkthroughs28_day,0)
      end as confirmed_walkthroughs
    , case when cg.campaign_goal = 'CUSTOMER_ACQUISITION' then round(confirmed_walkthroughs * s.sample_rate_multiplier)
        when cg.campaign_goal = 'LOYALTY' then round(confirmed_walkthroughs * 1) -- no sampling required
        when cg.campaign_goal = 'LOST_CUSTOMER' then round(confirmed_walkthroughs * 1) -- no sampling required
        when cg.campaign_goal is null then round(confirmed_walkthroughs * s.sample_rate_multiplier) -- if unknown, default to sampling
        else round(confirmed_walkthroughs * s.sample_rate_multiplier)
      end as total_walkthroughs
    , ifnull(i.walkthroughs1_day,0) as confirmed_walkthroughs_1_day
    , case when cg.campaign_goal = 'CUSTOMER_ACQUISITION' then round(confirmed_walkthroughs_1_day * s.sample_rate_multiplier)
        when cg.campaign_goal = 'LOYALTY' then round(confirmed_walkthroughs_1_day * 1) -- no sampling required
        when cg.campaign_goal = 'LOST_CUSTOMER' then round(confirmed_walkthroughs_1_day * 1) -- no sampling required
        when cg.campaign_goal is null then round(confirmed_walkthroughs_1_day * s.sample_rate_multiplier) -- if unknown, default to sampling
        else round(confirmed_walkthroughs_1_day * s.sample_rate_multiplier)
      end as total_walkthroughs_1_day
    , ifnull(i.walkthroughs7_day,0) as confirmed_walkthroughs_7_day
    , case when cg.campaign_goal = 'CUSTOMER_ACQUISITION' then round(confirmed_walkthroughs_7_day * s.sample_rate_multiplier)
        when cg.campaign_goal = 'LOYALTY' then round(confirmed_walkthroughs_7_day * 1) -- no sampling required
        when cg.campaign_goal = 'LOST_CUSTOMER' then round(confirmed_walkthroughs_7_day * 1) -- no sampling required
        when cg.campaign_goal is null then round(confirmed_walkthroughs_7_day * s.sample_rate_multiplier) -- if unknown, default to sampling
         else round(confirmed_walkthroughs_7_day * s.sample_rate_multiplier)
      end as total_walkthroughs_7_day
    , ifnull(i.walkthroughs28_day,0) as confirmed_walkthroughs_28_day
    , case when cg.campaign_goal = 'CUSTOMER_ACQUISITION' then round(confirmed_walkthroughs_28_day * s.sample_rate_multiplier)
        when cg.campaign_goal = 'LOYALTY' then round(confirmed_walkthroughs_28_day * 1) -- no sampling required
        when cg.campaign_goal = 'LOST_CUSTOMER' then round(confirmed_walkthroughs_28_day * 1) -- no sampling required
        when cg.campaign_goal is null then round(confirmed_walkthroughs_28_day * s.sample_rate_multiplier) -- if unknown, default to sampling
        else round(confirmed_walkthroughs_28_day * s.sample_rate_multiplier)
      end as total_walkthroughs_28_day
    , i.insight_id as ad_insight_ids
  from ad_dates_ cd
  left join zenstag.ads.ad_insights i 
    on cd.ad_account_id = i.ad_account_id
    and cd.campaign_id = i.campaign_id
    and cd.ad_set_id = i.ad_set_id
    and cd.ad_id = i.ad_id
    and cd.insight_type = i.insight_type
    and cd.date = try_to_date(i.breakdown_value)
    and i.breakdown_type = 'DAILY'
  -- left join adsbiz_ a on i.ad_account_id = a.ad_account_id
  left join zenstag.ads.zenreach_campaign_records zcr
    on i.campaign_id = zcr.campaign_id
  left join zenstag.ads.zenreach_campaigns zc
    on zcr.zenreach_campaign_id = zc.zenreach_campaign_id
  left join zenstag.ads.zenreach_campaign_records_locations zcrl
    on zcr.zenreach_campaign_records_id = zcrl.zenreach_campaign_records_id
  left join campaign_goals_ cg on cd.campaign_id = cg.campaign_id
  -- join ad set goals to override campaign-goal for for mixed-type campaigns
  left join zenstag.ads.zenreach_ad_set_goals zasg on i.ad_set_id = zasg.ad_set_id
  -- left join zenstag.ads.sample_rates s on coalesce(a.business_id, zcrl.location_id) = s.business_id and cd.date = s.day
  left join zenstag.ads.sample_rates s on zcrl.location_id = s.business_id and cd.date = s.day
  left join zenstag.ads.campaigns c on i.campaign_id = c.campaign_id
  join zenstag.ads.ad_accounts ac
    on i.ad_account_id = ac.ad_account_id
  where ac.is_zenreach = TRUE
    and zcr.platform = 'FACEBOOK'
    and date <= dateadd(days, 29, max_date_with_spend) 
    and cd.date >= '2019-11-15'
    and i.ad_account_id <> '126959897820002' -- `Zenreach Partner` ad account, used for testing circa 2018
    and i.ad_account_id <> '438192360194940' -- Brixx POS tests
    and i.ad_account_id <> '577138613087658' -- Brixx POS tests
    and i.ad_account_id <> '3288408101232413' -- POS tests
    and i.ad_account_id <> '349581652365682' -- Test Ad Level Reporting Video
    and i.campaign_id not in (select campaign_id from custom_conversion_campaigns_)
)

-- ### WALKTHROUGHS VIA CUSTOM CONVERSIONS ###
, custom_conversion_locations_ as (
   select
         cc.account_id
         , oesc.custom_conversion_id
         , oesc.location_id as business_id
   from zenstag.ads.offline_event_set_conversions oesc
   left join zenstag.ads.custom_conversions cc on cc.custom_conversion_id = oesc.custom_conversion_id
)

-- Map campaign_ids to location_ids
, ad_account_campaign_locations_mapping as (
  select
      zcr.campaign_id
      , zcrl.location_id as business_id
  from zenstag.ads.zenreach_campaign_records zcr
  inner join zenstag.ads.zenreach_campaign_records_locations zcrl on zcr.zenreach_campaign_records_id = zcrl.zenreach_campaign_records_id
)

, custom_conversion_location_dates_ as (
  select 
    adr.ad_account_id
    , adr.campaign_id
    , adr.ad_set_id
    , adr.ad_id
    , ccl.business_id
    , ccl.custom_conversion_id
    , adr.min_insight_date
    , adr.max_insight_date
  from ad_date_range_ adr
  left join custom_conversion_locations_ ccl on adr.ad_account_id = ccl.account_id
  where adr.campaign_id in (select campaign_id from custom_conversion_campaigns_)
  and ccl.business_id in (select business_id from ad_account_campaign_locations_mapping lm where lm.campaign_id = adr.campaign_id)
)

, custom_conversion_ad_dates_ as (
  select *
  from all_dates_ d
       , custom_conversion_location_dates_ cdr
       , insight_types_ it
  where d.date >= cdr.min_insight_date and d.date <= max_insight_date
  order by campaign_id, date
)

, custom_conversion_ad_insights_ as (
  select 
      aiicc.custom_conv_id as custom_conversion_id
      , a.campaign_id
      , a.ad_set_id
      , aiicc.ad_id
      , aiicc.insight_type
      , try_to_date(aiicc.breakdown_value) as date
      , cg.campaign_goal
      , cg.fb_display_attribution_window as merchant_dashboard_attribution_window
      , coalesce(zasg.goal,cg.campaign_goal) as ad_set_goal
      , case when cg.fb_display_attribution_window = '1' then ifnull(aiicc.walkthroughs1_day,0)
          when cg.fb_display_attribution_window = '7' then ifnull(aiicc.walkthroughs7_day,0)
          when cg.fb_display_attribution_window = '28' then ifnull(aiicc.walkthroughs28_day,0)
        end as walkthroughs
      , ifnull(aiicc.walkthroughs1_day,0) as walkthroughs_1_day
      , ifnull(aiicc.walkthroughs7_day,0) as walkthroughs_7_day
      , ifnull(aiicc.walkthroughs28_day,0) as walkthroughs_28_day
      , aiicc.insights_ads_custom_id
  from zenstag.ads.ad_insights_custom_conversions aiicc
  left join zenstag.ads.ads a on aiicc.ad_id = a.ad_id
  left join zenstag.ads.zenreach_ad_set_goals zasg on a.ad_set_id = zasg.ad_set_id
  left join campaign_goals_ cg on a.campaign_id = cg.campaign_id
  where breakdown_type = 'DAILY'
)

, custom_conversion_walkthroughs_ as (
  select 
    ccad.ad_account_id
    , ccad.campaign_id
    , ccad.ad_set_id
    , ccad.ad_id
    , ccad.business_id
    , ccad.insight_type
    , ccad.date    
    , case when merchant_dashboard_attribution_window = '1' then ifnull(ccai.walkthroughs_1_day,0)
        when merchant_dashboard_attribution_window = '7' then ifnull(ccai.walkthroughs_7_day,0)
        when merchant_dashboard_attribution_window = '28' then ifnull(ccai.walkthroughs_28_day,0)
      end as confirmed_walkthroughs
    , s.sample_rate_multiplier as sample_rate_multiplier
    , case when ad_set_goal = 'CUSTOMER_ACQUISITION' then round(ifnull(confirmed_walkthroughs,0) * s.sample_rate_multiplier)
        when ad_set_goal = 'LOYALTY' then round(ifnull(confirmed_walkthroughs,0) * 1) -- no sampling required
        when ad_set_goal = 'LOST_CUSTOMER' then round(ifnull(confirmed_walkthroughs,0) * 1) -- no sampling required
        when ad_set_goal is null then round(ifnull(confirmed_walkthroughs,0) * s.sample_rate_multiplier) -- if unknown, default to sampling
        else round(ifnull(confirmed_walkthroughs,0) * s.sample_rate_multiplier)
      end as total_walkthroughs
    , ifnull(ccai.walkthroughs_1_day,0) as confirmed_walkthroughs_1_day
    , case when ad_set_goal = 'CUSTOMER_ACQUISITION' then round(ifnull(ccai.walkthroughs_1_day,0) * s.sample_rate_multiplier)
        when ad_set_goal = 'LOYALTY' then round(ifnull(ccai.walkthroughs_1_day,0) * 1) -- no sampling required
        when ad_set_goal = 'LOST_CUSTOMER' then round(ifnull(ccai.walkthroughs_1_day,0) * 1) -- no sampling required
        when ad_set_goal is null then round(ifnull(ccai.walkthroughs_1_day,0) * s.sample_rate_multiplier) -- if unknown, default to sampling
        else round(ifnull(ccai.walkthroughs_1_day,0) * s.sample_rate_multiplier)
      end as total_walkthroughs_1_day
    , ifnull(ccai.walkthroughs_7_day,0) as confirmed_walkthroughs_7_day
    , case when ad_set_goal = 'CUSTOMER_ACQUISITION' then round(ifnull(ccai.walkthroughs_7_day,0) * s.sample_rate_multiplier)
        when ad_set_goal = 'LOYALTY' then round(ifnull(ccai.walkthroughs_7_day,0) * 1) -- no sampling required
        when ad_set_goal = 'LOST_CUSTOMER' then round(ifnull(ccai.walkthroughs_7_day,0) * 1) -- no sampling required
        when ad_set_goal is null then round(ifnull(ccai.walkthroughs_7_day,0) * s.sample_rate_multiplier) -- if unknown, default to sampling
        else round(ifnull(ccai.walkthroughs_7_day,0) * s.sample_rate_multiplier)
      end as total_walkthroughs_7_day
    , ifnull(ccai.walkthroughs_28_day,0) as confirmed_walkthroughs_28_day
    , case when ad_set_goal = 'CUSTOMER_ACQUISITION' then round(ifnull(ccai.walkthroughs_28_day,0) * s.sample_rate_multiplier)
        when ad_set_goal = 'LOYALTY' then round(ifnull(ccai.walkthroughs_28_day,0) * 1) -- no sampling required
        when ad_set_goal = 'LOST_CUSTOMER' then round(ifnull(ccai.walkthroughs_28_day,0) * 1) -- no sampling required
        when ad_set_goal is null then round(ifnull(ccai.walkthroughs_28_day,0) * s.sample_rate_multiplier) -- if unknown, default to sampling
        else round(ifnull(ccai.walkthroughs_28_day,0) * s.sample_rate_multiplier)
      end as total_walkthroughs_28_day
  from custom_conversion_ad_dates_ ccad
  left join custom_conversion_ad_insights_ ccai
   on ccad.custom_conversion_id = ccai.custom_conversion_id
          and ccad.campaign_id = ccai.campaign_id
          and ccad.ad_set_id = ccai.ad_set_id
          and ccad.ad_id = ccai.ad_id
          and ccad.insight_type = ccai.insight_type
          and ccad.date = ccai.date
  left join zenstag.ads.sample_rates s on ccad.business_id = s.business_id and ccad.date = s.day 
)

select ad_account_id, campaign_id, ad_set_id, ad_id, business_id, insight_type, date, confirmed_walkthroughs, sample_rate_multiplier, total_walkthroughs, confirmed_walkthroughs_1_day, total_walkthroughs_1_day, confirmed_walkthroughs_7_day, total_walkthroughs_7_day, confirmed_walkthroughs_28_day, total_walkthroughs_28_day
from ad_walkthroughs_after_2019_11_15_
union
select ad_account_id, campaign_id, ad_set_id, ad_id, business_id, insight_type, date, confirmed_walkthroughs, sample_rate_multiplier, total_walkthroughs, confirmed_walkthroughs_1_day, total_walkthroughs_1_day, confirmed_walkthroughs_7_day, total_walkthroughs_7_day, confirmed_walkthroughs_28_day, total_walkthroughs_28_day
from custom_conversion_walkthroughs_;
