CREATE OR REPLACE VIEW ZENPROD.ADS.FACEBOOK_CAMPAIGN_WALKTHROUGHS COMMENT='IN DEVELOPMENT 2020-07-16 - unified ads reporting view for camapaign-level location walkthroughs reported daily'
AS
-- generate all dates back 10 years
with all_dates_ as (
  SELECT DATEADD ( DAY, '-' || ROW_NUMBER() over ( ORDER BY NULL ) , DATEADD ( DAY, '+1',
    CURRENT_DATE() ) ) AS DATE
  FROM
    TABLE ( GENERATOR ( rowcount => ( 3650 ) ) )
)
-- calculate the first and last dates of insight_data attached to each campaign
, campaign_date_range_ as (
  select
    i.ad_account_id
    , i.campaign_id
    , min(try_to_date(breakdown_value)) as min_insight_date
    , max(try_to_date(breakdown_value)) as max_insight_date
    , ifnull(max(iff(spend_cents > 0, try_to_date(breakdown_value), NULL)), max_insight_date) as max_date_with_spend
  from zenprod.ads.insights i
  where breakdown_type = 'DAY' and insight_type = 'AGGREGATE'
  group by 1,2
)
-- values of insight_type column for join
, insight_types_ as (
  select *
  from (VALUES ('AGGREGATE')
                , ('UNIQUE')) as i (insight_type) 
)
-- create all combinations of date, ad account id, campaign_id and insight type
-- columns: date, ad_account_id, campaign_id, insight, min_insight_date, max_insight_date, insight_type
, campaign_dates_ as (
  select *
  from all_dates_ d
       , campaign_date_range_ cdr
       , insight_types_ it
  where d.date >= cdr.min_insight_date and d.date <= max_insight_date
  order by campaign_id, date
)
, custom_conversion_campaigns_ as (
  select 
      c.ad_account_id
      , icc.campaign_id
      , 'MULTILOC CC' as source
      , min(try_to_date(breakdown_value)) min_insight_date
      , max(try_to_date(breakdown_value)) max_insight_date
  from zenprod.ads.insights_custom_conversions icc
  left join zenprod.ads.campaigns c on icc.campaign_id = c.campaign_id
  left join zenprod.ads.zenreach_campaign_records zcr on zcr.campaign_id = c.campaign_id
  where breakdown_type = 'DAY'
  and zcr.zenreach_campaign_id in (select zenreach_campaign_id from zenprod.ads.zenreach_ad_account_campaigns)
  group by 1,2,3
)
, campaign_goals_ as (
  select
    zcr.campaign_id
  , zc.zenreach_campaign_id
  , zcr.platform
  , name
  , campaign_goal
  , creation_source
  , case when zcv.fb_display_attribution_window is null then '7'
    else zcv.fb_display_attribution_window
    end as fb_display_attribution_window
  from zenprod.ads.zenreach_campaigns zc
  left join zenprod.ads.zenreach_campaign_records zcr
      on zc.zenreach_campaign_id = zcr.zenreach_campaign_id
  left join zenprod.ads.zenreach_campaign_visibility zcv on
    zc.zenreach_campaign_id = zcv.zenreach_campaign_id
  where zc.status <> 'FAILED' and zcr.platform = 'FACEBOOK'
)


-- ### WALKTHROUGHS CALCULATED BEFORE 2019-11-15 WHEN AD-LEVEL DATA WAS COLLECTED ##
-- campaign insights w/ filled in missing dates
, walkthroughs_until_2019_11_15_ as (
  select 
    cd.ad_account_id
    -- , coalesce(a.business_id, zcrl.location_id) as business_id
    , zcrl.location_id as business_id
    , cd.campaign_id
    , c.name as campaign_name
    , cg.campaign_goal
    , cg.fb_display_attribution_window as merchant_dashboard_attribution_window
    , cd.insight_type
    , cd.date
    , case when cg.fb_display_attribution_window = '1' then ifnull(i.walkthroughs1_day,0)
        when cg.fb_display_attribution_window = '7' then ifnull(i.walkthroughs7_day,0)
        when cg.fb_display_attribution_window = '28' then ifnull(i.walkthroughs28_day,0)
      end as confirmed_walkthroughs
    , s.sample_rate_multiplier
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
    , i.insight_id
  from campaign_dates_ cd
  left join zenprod.ads.insights i 
    on cd.ad_account_id = i.ad_account_id
    and cd.campaign_id = i.campaign_id
    and cd.insight_type = i.insight_type
    and cd.date = try_to_date(i.breakdown_value)
    and i.breakdown_type = 'DAY'
  -- left join adsbiz_ a on i.ad_account_id = a.ad_account_id
  left join zenprod.ads.zenreach_campaign_records zcr
    on i.campaign_id = zcr.campaign_id
  left join zenprod.ads.zenreach_campaigns zc
    on zcr.zenreach_campaign_id = zc.zenreach_campaign_id
  left join zenprod.ads.zenreach_campaign_records_locations zcrl
    on zcr.zenreach_campaign_records_id = zcrl.zenreach_campaign_records_id
  left join zenprod.ads.campaigns c on cd.campaign_id = c.campaign_id
  join zenprod.ads.ad_accounts ac
    on i.ad_account_id = ac.ad_account_id
  -- left join zenprod.ads.sample_rates s on coalesce(a.business_id, zcrl.location_id) = s.business_id and cd.date = s.day
  left join zenprod.ads.sample_rates s on zcrl.location_id = s.business_id and cd.date = s.day
  left join campaign_goals_ cg on cd.campaign_id = cg.campaign_id
  where ac.is_zenreach = TRUE
    and zcr.platform = 'FACEBOOK'
    and date <= dateadd(days, 29, max_date_with_spend) 
    and cd.date < '2019-11-15'
    and i.ad_account_id <> '126959897820002' -- `Zenreach Partner` ad account, used for testing circa 2018
    and i.ad_account_id <> '438192360194940' -- Brixx POS tests
    and i.ad_account_id <> '577138613087658' -- Brixx POS tests
    and i.ad_account_id <> '3288408101232413' -- POS tests
    and i.ad_account_id <> '349581652365682' -- Test Ad Level Reporting Video
    and i.campaign_id not in (select campaign_id from custom_conversion_campaigns_)
)

-- ### WALKTHROUGHS AFTER 2019-11-15 WHEN AD-LEVEL DATA BECAME RELIABLY AVAILABLE ###
-- ### CALCULATE SAMPLING BY PULLING IN AD-SET LEVEL CAMPAIGN GOAL ###
, ad_set_walkthroughs_after_2019_11_15_ as (
  select 
    cd.ad_account_id
    -- , coalesce(a.business_id, zcrl.location_id) as business_id
    , zcrl.location_id as business_id
    , cd.campaign_id
    , c.name as campaign_name
    , cd.insight_type
    , cd.date
    , s.sample_rate_multiplier
    , coalesce(zasg.goal, cg.campaign_goal) as ad_set_goal
    , sum(case when cg.fb_display_attribution_window = '1' then ifnull(i.walkthroughs1_day,0)
        when cg.fb_display_attribution_window = '7' then ifnull(i.walkthroughs7_day,0)
        when cg.fb_display_attribution_window = '28' then ifnull(i.walkthroughs28_day,0)
      end) as confirmed_walkthroughs
    , sum(case when ad_set_goal = 'CUSTOMER_ACQUISITION' and cg.fb_display_attribution_window = '1' then round(ifnull(i.walkthroughs1_day,0) * s.sample_rate_multiplier)
        when ad_set_goal = 'LOYALTY' and cg.fb_display_attribution_window = '1' then round(ifnull(i.walkthroughs1_day,0) * 1) -- no sampling required
        when ad_set_goal = 'LOST_CUSTOMER' and cg.fb_display_attribution_window = '1' then round(ifnull(i.walkthroughs1_day,0) * 1) -- no sampling required
        when ad_set_goal is null and cg.fb_display_attribution_window = '1' then round(ifnull(i.walkthroughs1_day,0) * s.sample_rate_multiplier) -- if unknown, default to sampling
        when cg.fb_display_attribution_window = '1' then round(ifnull(i.walkthroughs1_day,0) * s.sample_rate_multiplier)
      
        when ad_set_goal = 'CUSTOMER_ACQUISITION' and cg.fb_display_attribution_window = '7' then round(ifnull(i.walkthroughs7_day,0) * s.sample_rate_multiplier)
        when ad_set_goal = 'LOYALTY' and cg.fb_display_attribution_window = '7' then round(ifnull(i.walkthroughs7_day,0) * 1) -- no sampling required
        when ad_set_goal = 'LOST_CUSTOMER' and cg.fb_display_attribution_window = '7' then round(ifnull(i.walkthroughs7_day,0) * 1) -- no sampling required
        when ad_set_goal is null and cg.fb_display_attribution_window = '7' then round(ifnull(i.walkthroughs7_day,0) * s.sample_rate_multiplier) -- if unknown, default to sampling
        when cg.fb_display_attribution_window = '7' then round(ifnull(i.walkthroughs7_day,0) * s.sample_rate_multiplier)
      
        when ad_set_goal = 'CUSTOMER_ACQUISITION' and cg.fb_display_attribution_window = '28' then round(ifnull(i.walkthroughs28_day,0) * s.sample_rate_multiplier)
        when ad_set_goal = 'LOYALTY' and cg.fb_display_attribution_window = '28' then round(ifnull(i.walkthroughs28_day,0) * 1) -- no sampling required
        when ad_set_goal = 'LOST_CUSTOMER' and cg.fb_display_attribution_window = '28' then round(ifnull(i.walkthroughs28_day,0) * 1) -- no sampling required
        when ad_set_goal is null and cg.fb_display_attribution_window = '28' then round(ifnull(i.walkthroughs28_day,0) * s.sample_rate_multiplier) -- if unknown, default to sampling
        when cg.fb_display_attribution_window = '28' then round(ifnull(i.walkthroughs28_day,0) * s.sample_rate_multiplier)
      end ) as total_walkthroughs
    , sum(ifnull(i.walkthroughs1_day,0)) as confirmed_walkthroughs_1_day
    , sum(case when ad_set_goal = 'CUSTOMER_ACQUISITION' then round(ifnull(i.walkthroughs1_day,0) * s.sample_rate_multiplier)
        when ad_set_goal = 'LOYALTY' then round(ifnull(i.walkthroughs1_day,0) * 1) -- no sampling required
        when ad_set_goal = 'LOST_CUSTOMER' then round(ifnull(i.walkthroughs1_day,0) * 1) -- no sampling required
        when ad_set_goal is null then round(ifnull(i.walkthroughs1_day,0) * s.sample_rate_multiplier) -- if unknown, default to sampling
        else round(ifnull(i.walkthroughs1_day,0) * s.sample_rate_multiplier)
      end ) as total_walkthroughs_1_day
    , sum(ifnull(i.walkthroughs7_day,0)) as confirmed_walkthroughs_7_day
    , sum(case when ad_set_goal = 'CUSTOMER_ACQUISITION' then round(ifnull(i.walkthroughs7_day,0) * s.sample_rate_multiplier)
        when ad_set_goal = 'LOYALTY' then round(ifnull(i.walkthroughs7_day,0) * 1) -- no sampling required
        when ad_set_goal = 'LOST_CUSTOMER' then round(ifnull(i.walkthroughs7_day,0) * 1) -- no sampling required
        when ad_set_goal is null then round(ifnull(i.walkthroughs7_day,0) * s.sample_rate_multiplier) -- if unknown, default to sampling
        else round(ifnull(i.walkthroughs7_day,0) * s.sample_rate_multiplier)
      end ) as total_walkthroughs_7_day
    , sum(ifnull(i.walkthroughs28_day,0)) as confirmed_walkthroughs_28_day
    , sum(case when ad_set_goal = 'CUSTOMER_ACQUISITION' then round(ifnull(i.walkthroughs28_day,0) * s.sample_rate_multiplier)
        when ad_set_goal = 'LOYALTY' then round(ifnull(i.walkthroughs28_day,0) * 1) -- no sampling required
        when ad_set_goal = 'LOST_CUSTOMER' then round(ifnull(i.walkthroughs28_day,0) * 1) -- no sampling required
        when ad_set_goal is null then round(ifnull(i.walkthroughs28_day,0) * s.sample_rate_multiplier) -- if unknown, default to sampling
        else round(ifnull(i.walkthroughs28_day,0) * s.sample_rate_multiplier)
      end ) as total_walkthroughs_28_day
    , listagg(distinct i.insight_id, ', ') as ad_insight_ids
  from campaign_dates_ cd
  left join zenprod.ads.ad_insights i 
    on cd.ad_account_id = i.ad_account_id
    and cd.campaign_id = i.campaign_id
    and cd.insight_type = i.insight_type
    and cd.date = try_to_date(i.breakdown_value)
    and i.breakdown_type = 'DAILY'
  -- left join adsbiz_ a on i.ad_account_id = a.ad_account_id
  left join zenprod.ads.zenreach_campaign_records zcr
    on i.campaign_id = zcr.campaign_id
  left join zenprod.ads.zenreach_campaigns zc
    on zcr.zenreach_campaign_id = zc.zenreach_campaign_id
  left join zenprod.ads.zenreach_campaign_records_locations zcrl
    on zcr.zenreach_campaign_records_id = zcrl.zenreach_campaign_records_id
  left join campaign_goals_ cg on cd.campaign_id = cg.campaign_id
  -- join ad set goals to override campaign-goal for for mixed-type campaigns
  left join zenprod.ads.zenreach_ad_set_goals zasg on i.ad_set_id = zasg.ad_set_id
  -- left join zenprod.ads.sample_rates s on coalesce(a.business_id, zcrl.location_id) = s.business_id and cd.date = s.day
  left join zenprod.ads.sample_rates s on zcrl.location_id = s.business_id and cd.date = s.day
  left join zenprod.ads.campaigns c on i.campaign_id = c.campaign_id
  join zenprod.ads.ad_accounts ac
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
  group by 1,2,3,4,5,6,7,8
)
, walkthroughs_after_2019_11_15_ as (
  select 
    w.ad_account_id
    , w.business_id
    , w.campaign_id
    , c.name as campaign_name
    , cg.campaign_goal
    , cg.fb_display_attribution_window as merchant_dashboard_attribution_window
    , w.insight_type
    , w.date
    , count(distinct w.ad_set_goal) as n_ad_set_goals
    , sum(w.confirmed_walkthroughs) as confirmed_walkthroughs
    , max(w.sample_rate_multiplier) as sample_rate_multiplier
    , sum(w.total_walkthroughs) as total_walkthroughs
    , sum(w.confirmed_walkthroughs_1_day) as confirmed_walkthroughs_1_day
    , sum(w.total_walkthroughs_1_day) as total_walkthroughs_1_day
    , sum(w.confirmed_walkthroughs_7_day) as confirmed_walkthroughs_7_day
    , sum(w.total_walkthroughs_7_day) as total_walkthroughs_7_day
    , sum(w.confirmed_walkthroughs_28_day) as confirmed_walkthroughs_28_day
    , sum(w.total_walkthroughs_28_day) as total_walkthroughs_28_day
    , listagg(w.ad_insight_ids, ', ') as insight_id
  from ad_set_walkthroughs_after_2019_11_15_ w
  left join campaign_goals_ cg on w.campaign_id = cg.campaign_id
  left join zenprod.ads.campaigns c on w.campaign_id = c.campaign_id
  group by 1,2,3,4,5,6,7,8
)

-- ### WALKTHROUGHS VIA CUSTOM CONVERSIONS ###
, custom_conversion_locations_ as (
   select
         cc.account_id
         , oesc.custom_conversion_id
         , oesc.location_id as business_id
   from zenprod.ads.offline_event_set_conversions oesc
   left join zenprod.ads.custom_conversions cc on cc.custom_conversion_id = oesc.custom_conversion_id
)

-- Map campaign_ids to location_ids
, ad_account_campaign_locations_mapping as (
  select
      zcr.campaign_id
      , zcrl.location_id as business_id
  from zenprod.ads.zenreach_campaign_records zcr
  inner join zenprod.ads.zenreach_campaign_records_locations zcrl on zcr.zenreach_campaign_records_id = zcrl.zenreach_campaign_records_id
)

, custom_conversion_location_dates_ as (
  select 
    cdr.ad_account_id
    , cdr.campaign_id
    , ccl.business_id
    , ccl.custom_conversion_id
    , cdr.min_insight_date
    , cdr.max_insight_date
  from campaign_date_range_ cdr
  left join custom_conversion_locations_ ccl on cdr.ad_account_id = ccl.account_id
  where cdr.campaign_id in (select campaign_id from custom_conversion_campaigns_)
  and ccl.business_id in (select business_id from ad_account_campaign_locations_mapping lm where lm.campaign_id = cdr.campaign_id)
)
, custom_conversion_campaign_dates_ as (
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
  from zenprod.ads.ad_insights_custom_conversions aiicc
  left join zenprod.ads.ads a on aiicc.ad_id = a.ad_id
  left join zenprod.ads.zenreach_ad_set_goals zasg on a.ad_set_id = zasg.ad_set_id
  left join campaign_goals_ cg on a.campaign_id = cg.campaign_id
  where breakdown_type = 'DAILY'
)
, custom_conversion_walkthroughs_ as (
  select 
    cccd.ad_account_id
    , cccd.campaign_id
    , cccd.business_id
    , cccd.insight_type
    , cccd.date
    , sum(ifnull(ccad.walkthroughs,0)) as confirmed_walkthroughs
    , sum(s.sample_rate_multiplier) as sample_rate_multiplier
    , sum(case when ad_set_goal = 'CUSTOMER_ACQUISITION' then round(ifnull(ccad.walkthroughs,0) * s.sample_rate_multiplier)
         when ad_set_goal = 'LOYALTY' then round(ifnull(ccad.walkthroughs,0) * 1) -- no sampling required
         when ad_set_goal = 'LOST_CUSTOMER' then round(ifnull(ccad.walkthroughs,0) * 1) -- no sampling required
         when ad_set_goal is null then round(ifnull(ccad.walkthroughs,0) * s.sample_rate_multiplier) -- if unknown, default to sampling
         else round(ifnull(ccad.walkthroughs,0) * s.sample_rate_multiplier)
      end) as total_walkthroughs
    , sum(ifnull(ccad.walkthroughs_1_day,0)) as confirmed_walkthroughs_1_day
    , sum(case when ad_set_goal = 'CUSTOMER_ACQUISITION' then round(ifnull(ccad.walkthroughs_1_day,0) * s.sample_rate_multiplier)
         when ad_set_goal = 'LOYALTY' then round(ifnull(ccad.walkthroughs_1_day,0) * 1) -- no sampling required
         when ad_set_goal = 'LOST_CUSTOMER' then round(ifnull(ccad.walkthroughs_1_day,0) * 1) -- no sampling required
         when ad_set_goal is null then round(ifnull(ccad.walkthroughs_1_day,0) * s.sample_rate_multiplier) -- if unknown, default to sampling
         else round(ifnull(ccad.walkthroughs_1_day,0) * s.sample_rate_multiplier)
      end) as total_walkthroughs_1_day
    , sum(ifnull(ccad.walkthroughs_7_day,0)) as confirmed_walkthroughs_7_day
    , sum(case when ad_set_goal = 'CUSTOMER_ACQUISITION' then round(ifnull(ccad.walkthroughs_7_day,0) * s.sample_rate_multiplier)
         when ad_set_goal = 'LOYALTY' then round(ifnull(ccad.walkthroughs_7_day,0) * 1) -- no sampling required
         when ad_set_goal = 'LOST_CUSTOMER' then round(ifnull(ccad.walkthroughs_7_day,0) * 1) -- no sampling required
         when ad_set_goal is null then round(ifnull(ccad.walkthroughs_7_day,0) * s.sample_rate_multiplier) -- if unknown, default to sampling
         else round(ifnull(ccad.walkthroughs_7_day,0) * s.sample_rate_multiplier)
      end) as total_walkthroughs_7_day
    , sum(ifnull(ccad.walkthroughs_28_day,0)) as confirmed_walkthroughs_28_day
    , sum(case when ad_set_goal = 'CUSTOMER_ACQUISITION' then round(ifnull(ccad.walkthroughs_28_day,0) * s.sample_rate_multiplier)
         when ad_set_goal = 'LOYALTY' then round(ifnull(ccad.walkthroughs_28_day,0) * 1) -- no sampling required
         when ad_set_goal = 'LOST_CUSTOMER' then round(ifnull(ccad.walkthroughs_28_day,0) * 1) -- no sampling required
         when ad_set_goal is null then round(ifnull(ccad.walkthroughs_28_day,0) * s.sample_rate_multiplier) -- if unknown, default to sampling
         else round(ifnull(ccad.walkthroughs_28_day,0) * s.sample_rate_multiplier)
      end) as total_walkthroughs_28_day
  from custom_conversion_campaign_dates_ cccd
  left join custom_conversion_ad_insights_ ccad
   on cccd.custom_conversion_id = ccad.custom_conversion_id
          and cccd.campaign_id = ccad.campaign_id
          and cccd.insight_type = ccad.insight_type
          and cccd.date = ccad.date
  left join zenprod.ads.sample_rates s on cccd.business_id = s.business_id and cccd.date = s.day 
  group by 1,2,3,4,5
)

-- ### JOIN THREE SETS OF WALKTHROUGH DATA TOGETHER INTO ONE SUPER-TABLE ###
, campaign_walkthroughs_ as (
  select 
    ad_account_id
    , campaign_id
    , campaign_name
    , campaign_goal
    , merchant_dashboard_attribution_window
    , business_id
    , insight_type
    , date
    , confirmed_walkthroughs
    , sample_rate_multiplier
    , total_walkthroughs
    , confirmed_walkthroughs_1_day
    , total_walkthroughs_1_day
    , confirmed_walkthroughs_7_day
    , total_walkthroughs_7_day
    , confirmed_walkthroughs_28_day
    , total_walkthroughs_28_day
    , 'SINGLE-LOC CAMPAIGN-LEVEL' as campaign_data_source
  from walkthroughs_until_2019_11_15_ w1
  union
  select
    ad_account_id
    , campaign_id
    , campaign_name
    , campaign_goal
    , merchant_dashboard_attribution_window
    , business_id
    , insight_type
    , date
    , confirmed_walkthroughs
    , sample_rate_multiplier
    , total_walkthroughs
    , confirmed_walkthroughs_1_day
    , total_walkthroughs_1_day
    , confirmed_walkthroughs_7_day
    , total_walkthroughs_7_day
    , confirmed_walkthroughs_28_day
    , total_walkthroughs_28_day
    , 'SINGLE-LOC AD-LEVEL AGG' as campaign_data_source
  from walkthroughs_after_2019_11_15_ w2
  union
  select 
    ccw.ad_account_id
    , ccw.campaign_id
    , c.name as campaign_name
    , campaign_goal
    , fb_display_attribution_window as merchant_dashboard_attribution_window
    , ccw.business_id
    , ccw.insight_type
    , ccw.date
    , ccw.confirmed_walkthroughs
    , ccw.sample_rate_multiplier
    , ccw.total_walkthroughs
    , ccw.confirmed_walkthroughs_1_day
    , ccw.total_walkthroughs_1_day
    , ccw.confirmed_walkthroughs_7_day
    , ccw.total_walkthroughs_7_day
    , ccw.confirmed_walkthroughs_28_day
    , ccw.total_walkthroughs_28_day
    , 'MULTILOC CUSTOM CONVERSIONS' as campaign_data_source
  from custom_conversion_walkthroughs_ ccw
  left join campaign_goals_ cg on ccw.campaign_id = cg.campaign_id
  left join zenprod.ads.campaigns c on ccw.campaign_id = c.campaign_id
)

select *
from campaign_walkthroughs_
order by campaign_id, date, insight_type;
