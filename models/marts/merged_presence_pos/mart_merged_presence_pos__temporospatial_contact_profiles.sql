{{ config(materialized='table') }}

with txs_ as (
  select *
  from {{ref('mart_merged_presence_pos__transactions')}}
  where pos_payment_method = 'CARD' -- only attempt matches on CC mart_merged_presence_pos__transactions
        and pos_name is not null
)
, pos_sightings_ as (
  select 
  tx.business_id
  , tx.pos_time_utc
  , tx.pos_time
  , tx.pos_customer_id
  , tx.pos_name
  , tx.pos_email
  , tx.pos_tx_count
  , s.sighting_id
  , s.classification
  , s.start_time
  , s.end_time
  , s.dwell_time
  , s.max_rssi
  , s.client_macs
  , s.total_device_dwell_time
  , s.total_device_sightings
  , s.contact
  , s.fullname
  , s.contact_alt_1
  , s.fullname_alt_1
  , s.contact_alt_2
  , s.fullname_alt_2
  , temporospatial_score(pos_name, pos_email, fullname, contact) as s1
  , temporospatial_score(pos_name, pos_email, fullname_alt_1, contact_alt_1) as s2
  , temporospatial_score(pos_name, pos_email, fullname_alt_2, contact_alt_2) as s3
  , least(s1
         ,s2
         ,s3) name_score
  from txs_ tx
       , {{ref('mart_merged_presence_pos__enriched_sightings')}} s
  -- WHERE tx.pos_time_utc between dateadd(hours, -1*$temporal_buffer, s.start_time) and dateadd(hours, $temporal_buffer, s.end_time)
  --                           and tx.business_id = s.business_id
  WHERE tx.pos_time between dateadd(hours, -1*2, s.start_time) and dateadd(hours, 2, s.end_time)
                            and tx.business_id = s.business_id
)
, matched_pos_ as (
  select
      client_macs
      , contact as zenreach_contact
      , fullname as zenreach_fullname
      , contact_alt_1 as zenreach_contact_alt_1
      , fullname_alt_1 as zenreach_fullname_alt_1
      , contact_alt_2 as zenreach_contact_alt_2
      , fullname_alt_2 as zenreach_fullname_alt_2
      , pos_customer_id
      , pos_email
      , pos_name
      , pos_tx_count
      -- these distincts are a bit of a hack to get around sightings that have multiple client macs
      -- hoping that two separate sightings don't have precisely the same dwell time, which isn't a guarantee
      , count(distinct sighting_id) matches
      , iff(max(total_device_dwell_time) = 0, NULL, sum(distinct dwell_time) / max(total_device_dwell_time)) dwell_time_ratio
      , iff(max(pos_tx_count) = 0, NULL, count(distinct sighting_id) / max(pos_tx_count)) as match_frequency_ratio
      , ifnull(dwell_time_ratio * match_frequency_ratio, 0) as match_score
      , avg(name_score) as name_score
      , min(pos_time) first_tx
      , max(pos_time) last_tx
      , listagg(distinct business_id, ', ') within group (ORDER BY business_id) as locations
      , listagg(distinct sighting_id, ', ') within group (ORDER BY sighting_id)  as sighting_ids
  from pos_sightings_
  group by 1,2,3,4,5,6,7,8,9,10,11
)
select distinct
    client_macs
    , zenreach_contact
    , zenreach_fullname
    , zenreach_contact_alt_1
    , zenreach_fullname_alt_1
    , zenreach_contact_alt_2
    , zenreach_fullname_alt_2
    , pos_customer_id
    , pos_email
    , pos_name
    , 'temporospatial' as source
    , locations
    , matches as temporospatial_matches
    , sighting_ids
    , current_timestamp() as created_at
from matched_pos_
where 
      -- TODO: Name score could certainly use refinement, especially around names that are very short (e.g. Ng)
      -- or names that are very common (e.g. Smith)
      name_score <= 0
      -- or strong temporospatial match & first name match
      or (pos_tx_count >= 2 and match_score > 0.25 and matches >= 2 and name_score <= 1)
