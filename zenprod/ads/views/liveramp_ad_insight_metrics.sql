CREATE OR REPLACE VIEW ZENPROD.ADS.LIVERAMP_AD_INSIGHT_METRICS COMMENT='IN DEVELOPMENT 2020-09-22 - liveramp ads reporting view for ad-level metrics such as impressions, clicks & spend reported daily'
AS

-- get parent_ids and names from CRM hiearchy
with parents_ as (
    select distinct
        parent_id
        , parent_name
    from zenprod.crm.businessprofile_hierarchy
)

-- get all zenreach liveramp ad accounts and include the parent info
, liveramp_ad_accounts_ as (
    select
        account_id as parent_id
        , p.parent_name as parent_name
        , ad_account_id
     from zenprod.ads.zenreach_ad_accounts zaa
     inner join parents_ p
        on zaa.account_id = p.parent_id
     where platform = 'LIVERAMP'
)

-- get all campaign info down to the creative level
, liveramp_zenreach_campaigns_ as (
    select
        zaa.account_id as parent_id
        , p.parent_name
        , zaa.ad_account_id
        , zaac.zenreach_campaign_id
        , zcr.zenreach_campaign_records_id
        , lrc.id as campaign_uuid
        , lrc.campaign_id
        , lrc.name as campaign_name
        , zc.campaign_goal
        , lrag.id as ad_group_uuid
        , lrag.ad_group_id
        , lrag.name as ad_group_name
        , ifnull(zaag.goal,zc.campaign_goal) as ad_group_goal
        , lrcr.id as creative_uuid
        , lrcr.creative_id
        , lrcr.name as creative_name
    from zenprod.ads.zenreach_ad_accounts zaa
    inner join parents_ p
        on zaa.account_id = p.parent_id
    inner join zenprod.ads.zenreach_ad_account_campaigns zaac
        on zaa.ad_account_id = zaac.ad_account_id
    inner join zenprod.ads.zenreach_campaigns zc
        on zaac.zenreach_campaign_id = zc.zenreach_campaign_id
    inner join zenprod.ads.zenreach_campaign_records zcr
        on zc.zenreach_campaign_id = zcr.zenreach_campaign_id
    inner join zenprod.ads.lr_campaigns lrc
        on zcr.campaign_id = lrc.campaign_id
    inner join zenprod.ads.lr_ad_groups lrag
        on lrc.id = lrag.campaign_id
    inner join zenprod.ads.lr_creatives lrcr
        on lrag.id = lrcr.ad_group_id
    left join zenprod.ads.zenreach_ad_set_goals zaag
        on zcr.zenreach_campaign_records_id = zaag.zenreach_campaign_records_id
        and lrag.ad_group_id = zaag.ad_set_id
    
    where zaa.platform = 'LIVERAMP'
        and zc.status = 'CREATED'
        and zcr.creation_status = 'CREATED'
  
)

-- generate all dates back 10 years
, all_dates_ as (
  SELECT DATEADD ( DAY, '-' || ROW_NUMBER() over ( ORDER BY NULL ) , DATEADD ( DAY, '+1',
    CURRENT_DATE() ) ) AS DATE
  FROM
    TABLE ( GENERATOR ( rowcount => ( 3650 ) ) )
)

-- get all dates that the campaign has metrics and spend for
, campaign_dates_ as (
  select
    lzc.campaign_uuid
    , lic.date
    , sum(lic.cost_cents) as cost_cents
  from zenprod.ads.lr_insights_creatives lic
  inner join liveramp_zenreach_campaigns_ lzc
    on lic.creative_id = lzc.creative_uuid
  group by 1,2
)

-- get all dates with walkthroughs
, walkthrough_dates_ as (
  select
    lzc.campaign_uuid
    , liw.sighting_day as date
    , 0 as cost_cents
  from zenprod.ads.lr_impressions_walkthroughs_by_day liw
  inner join liveramp_zenreach_campaigns_ lzc
    on liw.creative_id = lzc.creative_uuid
  group by 1,2
)

-- combine the campaign running dates and walkthrough dates
, liveramp_campaign_date_range_ as (
  select
    campaign_uuid
    , min(try_to_date(date)) as min_insight_date
    , max(try_to_date(date)) as max_insight_date
    , ifnull(max(iff(cost_cents > 0, try_to_date(date), NULL)), max_insight_date) as max_date_with_spend
  from (select * from campaign_dates_ union select * from walkthrough_dates_)
  group by 1
)

-- generate creative date range based on the campaign running dates
, liveramp_creative_date_range_ as (
  select
    lcdr.campaign_uuid
    , creative_uuid
    , lcdr.min_insight_date
    , lcdr.max_insight_date
    , lcdr.max_date_with_spend
  from liveramp_campaign_date_range_ lcdr
  inner join liveramp_zenreach_campaigns_ lzc
    on lcdr.campaign_uuid = lzc.campaign_uuid
)

-- values of insight_type column for join
, liveramp_insight_types_ as (
  select *
  from (VALUES ('AGGREGATE')) as i (insight_type) 
)

-- generate all liveramp ad dates based on ranges defined above
, liveramp_ad_dates_ as (
  select *
  from all_dates_ d
       , liveramp_creative_date_range_ cdr
       , liveramp_insight_types_ it
  where 
    d.date >= cdr.min_insight_date 
    and d.date <= max_insight_date
  order by creative_uuid, date
)

-- get margins for all campaigns
, margins_ as (
  select
    *
    , lag(updated) over (partition by ZENREACH_CAMPAIGN_RECORDS_ID order by updated desc) as effective_through
  from zenprod.ads.zenreach_campaign_records_margins m
)

-- generate ad insights for all liveramp campaings
, liveramp_insights_ as (
  select
    lzc.parent_id
    , lzc.parent_name
    , lzc.ad_account_id
    , lzc.zenreach_campaign_id
    , lzc.zenreach_campaign_records_id
    , lzc.campaign_uuid
    , lzc.campaign_id
    , lzc.campaign_name
    , lzc.campaign_goal
    , lzc.ad_group_uuid
    , lzc.ad_group_id
    , lzc.ad_group_name
    , lzc.ad_group_goal
    , lzc.creative_uuid
    , lzc.creative_id
    , lzc.creative_name
    , lad.insight_type
    , lad.date
    , lad.max_date_with_spend
    , current_timestamp() as updated_at
    , sum(CAST(ifnull(i.impressions,0) AS int)) as impressions
    , sum(CAST(ifnull(i.clicks,0) AS int)) as clicks
    , sum(ifnull(i.cost_cents/100,0)) as platform_spend
  from liveramp_ad_dates_ lad
  inner join liveramp_zenreach_campaigns_ lzc
    on lad.creative_uuid = lzc.creative_uuid
  left join zenprod.ads.lr_insights_creatives i
    on lzc.creative_uuid = i.creative_id
    and lad.date = i.date
  where lad.date <= dateadd(days, 29, max_date_with_spend)
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
)

, liveramp_insights_with_margin_applied_ as (
  select li.*
    , m.margin_percent as margin
    , (platform_spend) / (1 - ifnull(m.margin_percent,0)) as investment_dollars
  from liveramp_insights_ li
  left join margins_ m
    on li.zenreach_campaign_records_id = m.zenreach_campaign_records_id
    and li.date >= m.updated
    and li.date < ifnull(m.effective_through, current_date())
)

select * from liveramp_insights_with_margin_applied_;
