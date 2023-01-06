{{ config(materialized='table') }}

with nearby_locations_ as (
    select 
      g_pos_loc.business_id business_id
      , b_pos.business_name business_name
      , g_local_loc.business_id local_business_id
      , b_local.business_name local_business_name
      -- calculate distance between lat/log of the target business and nearby businesses
      -- TODO: this will also fail if the target business does not have a geocode
      -- NOTE: this will miss nearby businesses that do not have geocodes, should check these are populated
      , haversine(g_pos_loc.latitude, g_pos_loc.longitude, g_local_loc.latitude, g_local_loc.longitude) km
    
    from {{ seed_or_ref( ref('stg_business_profiles__d_business_geocode'), 'seed_d_business_geocode') }} g_pos_loc

    join {{ seed_or_ref( ref('stg_crm__businessprofile_hierarchy'), 'seed_businessprofile_hierarchy') }} b_pos on g_pos_loc.business_id = b_pos.business_id

    join {{ seed_or_ref( ref('stg_business_profiles__d_business_geocode'), 'seed_d_business_geocode') }} g_local_loc

    join {{ seed_or_ref( ref('stg_crm__businessprofile_hierarchy'), 'seed_businessprofile_hierarchy') }} b_local on g_local_loc.business_id = b_local.business_id

    where g_pos_loc.business_id in (select business_id from {{ref('mart_merged_presence_pos__pos_locations')}})
          and km <= 25 -- max_radius 16 km = 25 miles. TODO: This should be adjusted by population density?
          -- and km <= 16 -- max_radius 16 km = 10 miles. TODO: This should be adjusted by population density.
         -- and km <= 8 -- max_radius 8 km = 5 miles. TODO: This should be adjusted by population density
)
-- this generates about 87,000,000 contacts as of 2020-11-15
, analytics_customer_ as (
    select 
        ac.business_id, c.contact, c.fullname

    from {{ seed_or_ref(ref('stg_crm__analytics_customer'), 'SEED_ANALYTICS_CUSTOMER') }} ac

    join {{ref('mart_merged_presence_pos__contact_details')}} c on clean_contact(ac.email) = c.contact
    where 
        ac.business_id is not null 
        and ac.business_id in (select local_business_id from nearby_locations_)
        and c.email_is_valid = TRUE 
        and email_score > 0.5
        -- requiring a portal was omiting a lot of pretty good looking Craftsman 
        -- this could be omited to reduce the size of the join
        -- and ac.tags like '%wifi%'
        and contact like '%@%' 
)
, crm_ as (
  select 
    nl.business_id
    , nl.business_name
    , ac.contact
    , ac.fullname
    , get_initials(ac.fullname) as initials
    , get_last_name(ac.fullname) as last_name
    , max(ac.business_id = nl.business_id) as in_business_network
    , listagg(distinct nl.local_business_name, ' | ') within group (order by nl.local_business_name) as included_in_business_crms
    , count(distinct nl.local_business_id) as n_business_crms
    -- from zenalytics.crm.analytics_customer ac
  from analytics_customer_ ac
  join nearby_locations_ nl on ac.business_id = nl.local_business_id
  where (contact like '%@%' or len(contact) <> 24) -- intended to delete entries where contact_info = business_id
  group by 1,2,3,4,5,6
)
select *, current_timestamp() as created_at
from crm_
