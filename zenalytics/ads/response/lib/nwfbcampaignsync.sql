---------------------------------------------------------------
-------------------------- NWFBCAMPAIGNSYNC -------------------
---------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};
use role &{rolename};

create or replace transient table &{dbname}.&{schemaname}.ad_accounts as
select 
$1 as ad_account_id,
$2 as name,
$3::boolean as is_zenreach,
$4::timestamp as created_time,
$5::timestamp as last_synced,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/ad_accounts_v3.csv
; 

create or replace transient table &{dbname}.&{schemaname}.ad_creatives as
select 
$1 as ad_creative_id,
$2 as ad_account_id,
$3 as name,
$4 as page_id,
$5 as object_type,
$6 as object_permalink_url,
$7 as instagram_actor_id,
$8 as instagram_permalink_url,
$9 as video_id,
$10 as status,
$11 as title,
$12 as body,
$13 as thumbnail,
$14 as image,
$15 as call_to_action_type,
$16 as link_description,
$17 as link,
$18::timestamp as last_synced,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/ad_creatives_v3.csv
; 


create or replace transient table &{dbname}.&{schemaname}.ad_sets as
select 
$1 as ad_set_id,
$2 as ad_account_id,
$3 as campaign_id,
$4 as name,
$5::timestamp as start_time,
$6::timestamp as stop_time,
$7::timestamp as created_time,
$8::timestamp as updated_time,
$9 as status,
$10 as effective_status,
$11 as optimization_goal,
$12 as billing_event,
$13 as bid_strategy,
$14 as daily_budget,
$15 as budget_remaining,
$16 as age_min,
$17 as age_max,
$18::boolean as has_facebook,
$19::boolean as has_instagram,
$20::timestamp as last_synced,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/ad_sets_v3.csv
; 


create or replace transient table &{dbname}.&{schemaname}.campaigns as
select
$1 as campaign_id,
$2 as ad_account_id,
$3 as name,
$4::timestamp as start_time,
$5::timestamp as stop_time,
$6::timestamp as created_time,
$7::timestamp as updated_time,
$8 as status,
$9 as effective_status,
$10 as objective,
$11::timestamp as last_synced,
$12 as daily_spend_cents,
$13 as lifetime_spend_cents,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/campaigns_v3.csv
; 

create or replace transient table &{dbname}.&{schemaname}.ads as
select
$1 as ad_id,
$2 as ad_account_id,
$3 as campaign_id,
$4 as ad_set_id,
$5 as ad_creative_id,
$6 as name,
$7::timestamp as created_time,
$8::timestamp as updated_time,
$9 as status,
$10 as effective_status,
$11::timestamp as last_synced,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/ads_v3.csv
; 


create or replace transient table &{dbname}.&{schemaname}.insights as
select
$1 as insight_id,
$2 as ad_account_id,
$3 as campaign_id,
$4 as insight_type,
$5 as breakdown_type,
$6 as breakdown_value,
$7::integer as impressions,
$8::integer as clicks,
$9::integer as walkthroughs,
$10 as engagement,
$11::integer as spend_cents,
$12::timestamp as last_synced,
$31::integer as walkthroughs1_day,
$32::integer as walkthroughs7_day,
$33::integer as walkthroughs28_day,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/insights_v3.csv
;


create or replace transient table &{dbname}.&{schemaname}.ad_set_insights as
select
$1 as insight_id,
$2 as ad_account_id,
$3 as campaign_id,
$4 as ad_set_id,
$5 as insight_type,
$6 as breakdown_type,
$7 as breakdown_value,
$8::integer as impressions,
$9::integer as clicks,
$10::integer as walkthroughs,
$11 as engagement,
$12::integer as spend_cents,
$13::integer as thru_plays,
$14::integer as video_plays_at_25_perc,
$15::integer as video_plays_at_50_perc,
$16::integer as video_plays_at_75_perc,
$17::integer as video_plays_at_95_perc,
$18::integer as video_plays_at_100_perc,
$19::integer as video_plays,
$20::integer as video_average_play_time,
$21::integer as instant_experience_view_time,
$22::integer as instant_experience_view_perc,
$23::integer as link_clicks,
$24::integer as outbound_clicks,
$25::integer as instant_experience_clicks_to_open,
$26::integer as instant_experience_clicks_to_start,
$27::integer as instant_experience_outbound_clicks,
$28::integer as e_cpm_cents,
$29::timestamp as last_synced,
$30::integer as walkthroughs1_day,
$31::integer as walkthroughs7_day,
$32::integer as walkthroughs28_day,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/insights_ad_sets_v3.csv
;


create or replace transient table &{dbname}.&{schemaname}.ad_insights as
select
$1 as insight_id,
$2 as ad_account_id,
$3 as campaign_id,
$4 as ad_set_id,
$5 as ad_id,
$6 as insight_type,
$7 as breakdown_type,
$8 as breakdown_value,
$9::integer as impressions,
$10::integer as clicks,
$11::integer as walkthroughs,
$12 as engagement,
$13::integer as spend_cents,
$14::integer as thru_plays,
$15::integer as video_plays_at_25_perc,
$16::integer as video_plays_at_50_perc,
$17::integer as video_plays_at_75_perc,
$18::integer as video_plays_at_95_perc,
$19::integer as video_plays_at_100_perc,
$20::integer as video_plays,
$21::integer as video_average_play_time,
$22::integer as instant_experience_view_time,
$23::integer as instant_experience_view_perc,
$24::integer as link_clicks,
$25::integer as outbound_clicks,
$26::integer as instant_experience_clicks_to_open,
$27::integer as instant_experience_clicks_to_start,
$28::integer as instant_experience_outbound_clicks,
$29::integer as e_cpm_cents,
$30::timestamp as last_synced,
$31::integer as walkthroughs1_day,
$32::integer as walkthroughs7_day,
$33::integer as walkthroughs28_day,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/insights_ads_v3.csv
;
  

create or replace transient table &{dbname}.&{schemaname}.offline_event_sets as
select
$1 as offline_event_sets_id,
$2 as best_account_id,
split_part($3,' ',1) as business_id,
replace(replace($3,split_part($3,' ',1)),'_',' ') as business_name,
$4 as description,
$5::boolean as is_auto_assign,
$6::timestamp as creation_time,
$7::timestamp as event_time_min,
$8::timestamp as event_time_max,
$9::timestamp as last_upload_time,
parse_json($10)::variant as event_stats,
$11::integer as valids,
$12::integer as duplicates,
$13::integer as matches,
$14::integer as match_rate,
$15 as last_synced, 
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/offline_event_sets_v3.csv
;

create or replace transient table &{dbname}.&{schemaname}.pages_hours as
select
$1 as pages_hour_id,
$2 as page_id,
$3 as hour_key,
$4 as hour_value,
$5::timestamp as last_synced,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/pages_hours_v3.csv
;

create or replace transient table &{dbname}.&{schemaname}.pages_instagram_accounts as
select 
$1 as pages_instagram_accounts_id,
$2::integer as page_id,
$3::integer as instagram_id,
$4 as instagram_name,
$5::timestamp as last_synced,  
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/pages_instagram_accounts_v3.csv
;


create or replace transient table &{dbname}.&{schemaname}.pages as
select 
$1 as page_id,
$2 as name,
$3 as street,
$4 as city,
$5 as state,
$6 as zip,
$7 as country,
$8::float as latitude,
$9::float as longitude,
$10::boolean as is_always_open,
$11::timestamp as last_synced,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/pages_v3.csv
;

create or replace transient table &{dbname}.&{schemaname}.pages_verticals as
select 
$1 as page_vertical_id,
$2::integer as page_id,
$3::integer as vertical_id,
$4 as vertical_name,
$5::timestamp as last_synced,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/pages_verticals_v3.csv
;


create or replace transient table &{dbname}.&{schemaname}.uploaded_sightings
as
    select
    $1 as uploaded_sightings_id,
    $2 as sighting_id,
    $3 as offline_event_set_id,
    $4 as business_id,
    $5::timestamp as end_time,
    $6::timestamp as uploaded,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/uploaded_sightings.csv
;

create or replace transient table &{dbname}.&{schemaname}.uploaded_sightings_custom_data as
select 
$1 as uploaded_sightings_id,
$2 as field,
$3 as value,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/uploaded_sightings_custom_data.csv
;

create or replace transient table &{dbname}.&{schemaname}.custom_conversions as
select 
$1 as custom_conversion_id,
$2 as account_id,
$3 as custom_event_type,
$4 as name,
$5::timestamp as last_synced,
$6::boolean as is_archived,
$7 as offline_event_set_id,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/view_custom_conversions_snowflake.csv
;

create or replace transient table &{dbname}.&{schemaname}.ad_set_insights_custom_conversions as
select
$1 as insights_ad_sets_custom_id,
$2 as custom_conv_id,
$3 as ad_set_id,
$4::integer as walkthroughs,
$5 as insight_type,
$6 as breakdown_type,
$7 as breakdown_value,
$8::timestamp as last_synced,
$9::integer as walkthroughs1_day,
$10::integer as walkthroughs7_day,
$11::integer as walkthroughs28_day,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/insights_ad_sets_custom_v3.csv
;

create or replace transient table &{dbname}.&{schemaname}.ad_insights_custom_conversions as
select
$1 as insights_ads_custom_id,
$2 as custom_conv_id,
$3 as ad_id,
$4::integer as walkthroughs,
$5 as insight_type,
$6 as breakdown_type,
$7 as breakdown_value,
$8::timestamp as last_synced,
$9::integer as walkthroughs1_day,
$10::integer as walkthroughs7_day,
$11::integer as walkthroughs28_day,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/insights_ads_custom_v3.csv
;

create or replace transient table &{dbname}.&{schemaname}.insights_custom_conversions as
select 
$1 as insights_custom_id,
$2 as custom_conv_id,
$3 as campaign_id,
$4::integer as walkthroughs,
$5 as insight_type,
$6 as breakdown_type,
$7 as breakdown_value,
$8::timestamp as last_synced,
$9::integer as walkthroughs1_day,
$10::integer as walkthroughs7_day,
$11::integer as walkthroughs28_day,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/insights_custom_v3.csv
;

