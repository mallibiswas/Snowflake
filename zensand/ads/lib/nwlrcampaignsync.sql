---------------------------------------------------------------
-------------------------- NWLRCAMPAIGNSYNC -------------------
---------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};
use role &{rolename};

create or replace transient table &{dbname}.&{schemaname}.lr_processed_exports as
select
$1 as message_id,
$2 as url,
$3::timestamp as processed,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/processed_exports.csv
;

create or replace transient table &{dbname}.&{schemaname}.lr_advertisers as
select
$1 as id,
$2 as advertiser_id,
$3 as name,
$4::timestamp as created,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/advertisers.csv
;

create or replace transient table &{dbname}.&{schemaname}.lr_campaigns as
select
$1 as id,
$2 as campaign_id,
$3 as advertiser_id,
$4 as name,
$5::timestamp as created,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/campaigns.csv
;

create or replace transient table &{dbname}.&{schemaname}.lr_ad_groups as
select
$1 as id,
$2 as ad_group_id,
$3 as campaign_id,
$4 as name,
$5::timestamp as created,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/ad_groups.csv
;

create or replace transient table &{dbname}.&{schemaname}.lr_creatives as
select
$1 as id,
$2 as creative_id,
$3 as ad_group_id,
$4 as name,
$5::timestamp as created,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/creatives.csv
;

create or replace transient table &{dbname}.&{schemaname}.lr_insights_creatives as
select
$1 as creative_id,
$2 as message_id,
$3::date as date,
$4 as device_type,
$5 as media_type,
$6 as impressions,
$7 as clicks,
$8 as cost_cents,
$9 as impression_uniques,
$10 as player_starts,
$11 as player25perc_complete,
$12 as player50perc_complete,
$13 as player75perc_complete,
$14 as player_completed_views,
$15 as sampled_tracked_impressions,
$16 as sampled_viewed_impressions,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/insights_creatives.csv
;

create or replace transient table &{dbname}.&{schemaname}.lr_impressions_walkthroughs_by_day as
select
$1 as campaign_name,
$2 as campaign_id,
$3 as ad_group_name,
$4 as ad_group_id,
$5 as creative_name,
$6 as creative_id,
$7 as location_id,
$8::date as sighting_day,
$9 as aggregate7day,
$10 as aggregate14day,
$11 as aggregate28day,
$12 as unique7day,
$13 as unique14day,
$14 as unique28day,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/view_impressions_walkthroughs_by_day.csv
;
