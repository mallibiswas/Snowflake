---------------------------------------------------------------
-------------------------- NWOMNI -----------------------------
---------------------------------------------------------------

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};

create or replace transient table &{dbname}.&{schemaname}.zenreach_campaigns as
select
$1 as zenreach_campaign_id,
$2::timestamp as start_time,
$3::timestamp as end_time,
$4 as status,
$5::timestamp as created,
$6::timestamp as updated,
$7 as name,
$8::integer as daily_budget_cents,
$9 as creation_source,
$10 as campaign_goal,
$11::integer as total_budget_cents,
$12::boolean as is_io_mappable,
-- $13 as io_reason,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/view_zenreach_campaigns_snowflake.csv
;

create or replace transient table &{dbname}.&{schemaname}.zenreach_campaign_records as
select
$1 as zenreach_campaign_records_id,
$2 as zenreach_campaign_id,
$3 as campaign_id,
$4 as creation_status,
$5 as platform,
$6 as platform_account_id,
$7::integer as daily_budget_cents,
$8::integer as total_budget_cents,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/zenreach_campaign_records.csv
;

create or replace transient table &{dbname}.&{schemaname}.zenreach_campaign_records_locations as
select
$1 as zenreach_campaign_records_id,
$2 as location_id,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/zenreach_campaign_records_locations.csv
;

create or replace transient table &{dbname}.&{schemaname}.zenreach_campaign_records_margins as
select
$1 as campaign_id,
$2::float as margin_percent,
$3::timestamp as updated,
$4 as zenreach_campaign_records_id,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/view_zenreach_campaign_records_margins.csv
;

create or replace transient table &{dbname}.&{schemaname}.ams_campaign as
select
$1 as zenreach_campaign_id,
$2 as ads_io_id,
$3::timestamp as updated,
$4 as mapped_by,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/ams_campaign.csv
;

create or replace transient table &{dbname}.&{schemaname}.zenreach_ad_accounts as
select
$1 as ad_account_id,
$2 as name,
$3 as platform,
$4 as account_id,
$5::boolean as upload_enabled,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/ad_accounts.csv
;

create or replace transient table &{dbname}.&{schemaname}.zenreach_ad_account_locations as
select
$1 as ad_account_id,
$2 as location_id,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/ad_account_locations.csv
;

create or replace transient table &{dbname}.&{schemaname}.zenreach_ad_account_fb_config as
select
$1 as ad_account_id,
$2 as facebook_ad_account_id,
$3 as offline_event_set_id,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/ad_account_fb_config.csv
;

create or replace transient table &{dbname}.&{schemaname}.zenreach_ad_account_campaigns as
select
$1 as ad_account_id,
$2 as zenreach_campaign_id,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/ad_account_campaigns.csv
;

create or replace transient table &{dbname}.&{schemaname}.offline_event_set_conversions as
select
$1 as offline_event_set_id,
$2 as location_id,
$3 as custom_conversion_id,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/offline_event_set_conversions.csv
;

create or replace transient table &{dbname}.&{schemaname}.zenreach_campaign_visibility as
select
$1 as zenreach_campaign_id,
$2::boolean as is_visible_on_dashboard,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/zenreach_campaign_visibility.csv
;

create or replace transient table &{dbname}.&{schemaname}.zenreach_ad_set_goals as
select
$1 as zenreach_campaign_records_id,
$2 as ad_set_id,
$3 as goal,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/zenreach_ad_set_goals.csv
;

create or replace transient table &{dbname}.&{schemaname}.credit_reasons as
select
$1 as reason,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/credit_reasons.csv
;

create or replace transient table &{dbname}.&{schemaname}.credit_types as
select
$1 as type,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/credit_types.csv
;

create or replace transient table &{dbname}.&{schemaname}.ams_campaign_credits as
select
$1 as ams_campaign_credits_id,
$2 as ams_campaign_id,
$3 as reason,
$4 as value_type,
$5::float as value,
$6::timestamp as start_date,
$7::timestamp as end_date,
$8 as notes,
$9 as username,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/ams_campaign_credits.csv
;

-- create or replace transient table &{dbname}.&{schemaname}.io_reasons as
-- select
-- $1 as reason,
-- '&{asof_date}'::date as asof_date
-- FROM @&{stagename}/&{stagepath}/io_reasons.csv
-- ;
