-----------------------------------------------------------------------
--------------------- gainsight_create_metrics ------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};

ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;

create or replace temporary table &{dbname}._STAGING._gainsight_acct_attributes
as
WITH sfdc_user_acct as (select a.PARENTID,
                    a.name as account_name,
                    u.name,
                    a.RETENTION_ACCOUNT_MANAGER__C as acct_mgr_id,
                    NVL(NUMBER_OF_LOCATIONS__C,NVL(OF_LOCATIONS__C,0)) as NUMBER_OF_LOCATIONS__C,
                    NVL(OF_LOCATIONS__C,0) as OF_LOCATIONS__C,
                    a.id as salesforce_id,
                    u.id as sfdc_user_id,
                    REFERRING_PARTNER__C
                    from &{dbname}.SFDC.account a left join sfdc.user u
                    on a.RETENTION_ACCOUNT_MANAGER__C = u.id),

sfdc_metrics as (
    select sua1.salesforce_id,
        sua1.REFERRING_PARTNER__C,
        NVL(sua1.name, NVL(sua2.name,sua3.name)) as retention_acct_mgr,
        GREATEST(NVL(sua1.NUMBER_OF_LOCATIONS__C,0),NVL(sua2.NUMBER_OF_LOCATIONS__C,0),NVL(sua3.NUMBER_OF_LOCATIONS__C,0)) as max_sfdc_locs,
        case when sua1.NUMBER_OF_LOCATIONS__C >= 10 or
             sua1.OF_LOCATIONS__C >= 10 or
             sua2.NUMBER_OF_LOCATIONS__C >= 10 or
             sua2.OF_LOCATIONS__C >= 10 or
             sua3.NUMBER_OF_LOCATIONS__C >= 10 or
             sua3.OF_LOCATIONS__C >= 10 then True else False end as sfdc_managed_locs
    from sfdc_user_acct sua1
    left join sfdc_user_acct sua2 on sua1.salesforce_id = sua2.parentid
    left join sfdc_user_acct sua3 on sua2.salesforce_id = sua3.parentid),

num_locs_ams as (select account_id,
                  count(distinct LOCATION_ID) as num_all_time_locs_ams
                  from &{dbname}.AMS.LOCATION
                  group by 1),

location_list as (
    select
      bph.l1_id as parent_id,
      ams_acct.business_profile_id as ams_parent_id,
      ams_loc.LOCATION_ID,
      ams_loc.name as loc_name,
      ams_acct.ACCOUNT_ID,
      ams_acct.account_state,
      ams_acct.name as acct_name,
      sfdc_metrics.retention_acct_mgr,
      ams_loc.SALESFORCE_ID,
      ams_loc.BUSINESS_PROFILE_ID as business_id,
      case
        when ams_acct.PARTNER_ACCOUNT_ID is not null then 'Channel - AMS Gold/Silver'
        when sfdc_metrics.REFERRING_PARTNER__C is not null then 'Channel - Other'
        when sfdc_metrics.sfdc_managed_locs = True or
             num_locs_ams.num_all_time_locs_ams >= 10
             then 'Managed'
             else 'Programmatic'
             end as segment_type,
      ams_acct.PARTNER_ACCOUNT_ID as ams_partner_id,
      num_locs_ams.num_all_time_locs_ams,
      sfdc_metrics.max_sfdc_locs,
      GREATEST(nvl(sfdc_metrics.max_sfdc_locs,0), nvl(num_locs_ams.num_all_time_locs_ams,0)) as max_total_locs
from &{dbname}.AMS.location as ams_loc
inner join &{dbname}.CRM.businessprofile_hierarchy bph on bph.business_id = ams_loc.business_profile_id
left join &{dbname}.AMS.ACCOUNT as ams_acct on ams_acct.ACCOUNT_ID = ams_loc.ACCOUNT_ID
left join &{dbname}.AMS.PARTNER_ACCOUNT ams_partner on ams_partner.PARTNER_ACCOUNT_ID = ams_acct.PARTNER_ACCOUNT_ID
left join num_locs_ams on num_locs_ams.account_id = ams_loc.ACCOUNT_ID
left join sfdc_metrics on sfdc_metrics.SALESFORCE_ID = ams_acct.SALESFORCE_ID
)


select
ll.account_id,
ll.parent_id,
account_state,
acct_name,
segment_type,
ams_partner_id,
retention_acct_mgr,
max(num_all_time_locs_ams) as num_all_time_locs_ams,
max(max_total_locs) as max_total_locs
from location_list ll

group by 1,2,3,4,5,6,7
order by 1 asc;


create or replace temporary table &{dbname}._STAGING._gainsight_email_collections
as
WITH date_boundaries as (select dateadd(day, -62, current_date) as t_minus_62, dateadd(day, -31, current_date) as t_minus_31),
-- emails collected
email_counts as (
    select
        ac.business_id as parent_id,
        concat('https://my.zenreach.com/overview/?business=',ac.business_id) as dash_link,
        SUM(case when ac.date < t_minus_31 and ac.date >= t_minus_62 then emails else 0 end) as last_31_to_62_day_email_count,
        SUM(case when ac.date >= t_minus_31 then emails else 0 end) as last_31_day_email_count,
        sum(emails) as total_email_count
    from &{dbname}.CRM.ANALYTICS_COLLECTIONSTATS ac
    inner join ams.account as aa on ac.business_id = aa.business_profile_id
    inner join date_boundaries
    where ac.date is not null
    group by 1
),

-- targetable emails collected
targetable_emails as (
      select
          bh.parent_id,
          count(distinct c.email) as num_targetable_emails
      from
      	crm.businessprofile_hierarchy bh
      	inner join CRM.ANALYTICS_CUSTOMER c on bh.parent_id = c.business_id
      	inner join crm.portal_userprofile u on c.email = u.email
      	left join crm.gdpr_contact_blacklist cb on bh.parent_id = cb.account_id and u.userprofile_id = cb.contact_id
      	where contact_allowed = True
	and (u.email_is_valid <> False or (c.phone is not null and u.email_is_valid is null))
	and ( 	tags ilike '%wifi%' or
  		tags ilike '%importer%' or
  		tags ilike '%webform%' or
  		tags ilike '%dashboard_add%')
        and bh.parent_id is not null
      group by 1
)

select 	a.*, b.num_targetable_emails
from 	email_counts a, targetable_emails b
where 	a.parent_id = b.parent_id;


create or replace temporary table &{dbname}._staging._gainsight_subscription_attributes
as
WITH core as (
        -- core
        -- core
        select  s.account_id,
                p.name as core_package_name,
                count(*) as active_core_subs
        from zenalytics.ams.subscriptions_v2 s, ams.package p, ams.account a
        where subscription_state = 'ACTIVE'
        and s.account_id = a.account_id
        and a.migrated_date is null
        and s.package_id = p.package_Id
        and p.package_id not in (8,3)
        group by 1,2
        UNION ALL
        select  a.account_id,
                s.package as ads_package_name,
                count(*) as active_core_subs
        from zenalytics.bizint.subscription_ts s, zenalytics.ams.account a
        where s.account_id = a.v3_account_id and package != 'Ads'
        and subscription_active_ind = True
        group by 1,2
    ),
ads as (
    -- ads: accounts that have not been migrated
    -- v2 ads
    select  s.account_id,
            p.name as ads_package_name,
            count(case when subscription_state = 'ACTIVE' then subscription_id else null end) as active_ads_subs,
            to_date(MAX(case when subscription_state = 'CANCELLED' then s.updated else null end)) as last_ads_subs_cancelled_date
    from zenalytics.ams.subscriptions_v2 s, zenalytics.ams.package p, zenalytics.ams.account a
    where s.package_id = p.package_Id
    and s.account_id = a.account_id
    and a.migrated_date is null
    and p.package_id in (8,3)
    group by 1,2
    UNION ALL -- +v3 ads
    select  a.account_id,
            s.package as ads_package_name,
            count(case when subscription_active_ind = True then subscription_id else null end) as active_ads_subs,
            to_date(MAX(case when subscription_active_ind = False then s.subscription_cancelled_date else null end)) as last_ads_subs_cancelled_date
    from zenalytics.bizint.subscription_ts s, zenalytics.ams.account a
    where s.account_id = a.v3_account_id
    and package = 'Ads'
    group by 1,2
)
select  core.account_id,
        core.core_package_name,
        nvl(ads.active_ads_subs,0) as active_ads_subs,
        ads.last_ads_subs_cancelled_date
from core LEFT JOIN ads
on core.account_id = ads.account_id
;


------------- Calculate 30 day lost customers
create or replace temporary table zenalytics._staging._gainsight_lost_customer_attributes
as
with audience_visit_agg_ as (
  select
    b.parent_id
    , contact_info
    , count(*) as visit_count
    , min(start_time) as first_seen
    , max(end_time) as last_seen
    , max(case when known_to_merchant_account = TRUE then 1 else 0 end) as IN_BUSINESS_NETWORK
  from ZENALYTICS.PRESENCE.FINISHED_SIGHTINGS c
  left join ZENPROD.crm.PORTAL_BUSINESSPROFILE b
    on c.location_id = b.business_id
  where classification = 'WALKIN'
  AND contact_info IS NOT NULL
  and end_time > '2019-01-01'
  group by b.parent_id, contact_info
  )

select
  parent_id
  , count(*) as n_visitors
  , sum(IN_BUSINESS_NETWORK) as n_in_business_network
  , 1 - sum(IN_BUSINESS_NETWORK)/count(*) as perc_invisible
  , median(visit_count) as median_visit_count
  , count(*) * median(visit_count) * 20 as potential_value
from audience_visit_agg_ a
where last_seen <= dateadd(day, -1 * 30, current_date())
group by 1;

------------- Calculate 30 day Walk-Bys and total Walkbys from 2018-09-13 to date
create or replace temporary table zenalytics._staging._gainsight_walkby_metrics
as
WITH wbYTD as (
select  parent_id,
        sum(walkby_merchant) as walkby_merchant_ytd,
        sum(walkby_network) as walkby_network_ytd
        from zenalytics.presence.presence_sampling_stats a, crm.businessprofile_hierarchy b
        where a.business_id = b.business_id
        and date_hour >= '2019-01-01'
        group by parent_id
        ),
wb30d as  (
select  parent_id,
        sum(walkby_merchant) as walkby_merchant_30d,
        sum(walkby_network) as walkby_network_30d
        from zenalytics.presence.presence_sampling_stats a, crm.businessprofile_hierarchy b
        where a.business_id = b.business_id
        and date_hour >= dateadd(day, -31, current_date()) -- make 31 to compensate for 1 day lag on load time
        group by parent_id
        )
select  wbYTD.parent_id,
        wbYTD.walkby_merchant_ytd,
        wbYTD.walkby_network_ytd,
        wb30d.walkby_merchant_30d,
        wb30d.walkby_network_30d
from wbYTD LEFT JOIN wb30d on wbYTD.parent_id = wb30d.parent_id;


SET MAX_ASOF_DATE = (SELECT MAX(asof_date) FROM &{dbname}.&{schemaname}.&{tablename} where asof_date < current_date());

insert into zenalytics.public.gainsight_acct_attributes
(
PARENT_ID,
ACCOUNT_ID,
AMSACCOUNTSTATE,
STANDARDIZEDACCOUNTNAME,
ZENREACHBUSINESSSEGMENT,
AMS_PARTNER_ID,
RETENTIONACCOUNTMANAGER,
COREPRODUCTPACKAGE,
TOTALNUMBEROFLOCATIONS,
TOTALEMAILSCOLLECTEDALLTIME,
EMAILSCOLLECTEDLAST31DAYS,
EMAILSCOLLECTEDPREV31to62DAYS,
CURRENTACTIVEEMAILLISTSIZE,
CURRENTADSSUBSCRIPTIONS,
LASTADSSUBSCANCELLEDDATE,
NVISITORS,
NINBUSINESSNETWORK,
PERCINVISIBLE,
MEDIANVISITCOUNT,
POTENTIALVALUE,
WALKBYMERCHANTYTD,
WALKBYNETWORKYTD,
WALKBYMERCHANT30D,
WALKBYNETWORK30D,
ASOF_DATE,
LAST_ASOF_DATE)
select
    a.parent_id,
    a.account_id,
    a.account_state,
    TRIM(REGEXP_REPLACE(a.acct_name,'(parent|group|Corporate|llc|franchising|-|:|Account)','',1,0,'i')) as StandardizedAccountName,
    a.segment_type as ZenreachBusinessSegment,
    a.ams_partner_id,
    a.retention_acct_mgr as RetentionAccountManager,
    b.core_package_name as CoreProductPackage,
    MAX(GREATEST(a.num_all_time_locs_ams, a.max_total_locs)) as TotalNumberOfLocations,
    SUM(e.total_email_count) as TotalEmailsCollectedAllTime,
    SUM(e.last_31_day_email_count) as EmailCollectedLast31Days,
    SUM(e.last_31_to_62_day_email_count) as EmailCollectedPrev31to62Days,
    SUM(e.num_targetable_emails) as CurrentActiveEmailListSize,
    SUM(b.active_ads_subs) as CurrentAdssubscriptions,
    MAX(b.last_ads_subs_cancelled_date) as LastAdsSubsCancelledDate,
    SUM(n_visitors),
    SUM(n_in_business_network),
    MAX(perc_invisible),
    SUM(median_visit_count),
    SUM(potential_value),
    SUM(walkby_merchant_ytd),
    SUM(walkby_network_ytd),
    SUM(walkby_merchant_30d),
    SUM(walkby_network_30d),
    MAX(current_date) as asof_date,
    MAX(to_date($MAX_ASOF_DATE)) as LAST_ASOF_DATE
from zenalytics._staging._gainsight_acct_attributes a
left join zenalytics._staging._gainsight_subscription_attributes b on a.account_id = b.account_id
left join zenalytics._staging._gainsight_email_collections e on a.parent_id = e.parent_id
left join zenalytics._staging._gainsight_lost_customer_attributes l on a.parent_id = l.parent_id
left join zenalytics._staging._gainsight_walkby_metrics w on a.parent_id = w.parent_id
group by 1,2,3,4,5,6,7,8;
