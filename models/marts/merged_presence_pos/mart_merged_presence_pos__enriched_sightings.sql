{{ config(materialized='table') }}

with 
-- first flatten the sightings by client mac
sightings_ as (
  select 
      s.sighting_id
      , s.business_id
      , s.classification
      , start_time as start_time_utc
      , end_time as end_time_utc
      , dateadd(hours, g.timezone_utc_offset, s.start_time) as start_time
      , dateadd(hours, g.timezone_utc_offset, s.end_time) as end_time
      , s.dwell_time
      , s.max_rssi
      , s.client_mac_info
      , clean_contact(contact_info) as sighting_contact
      , upper(replace(c.value:client_mac::string, ':')) as client_mac
      , sum(dwell_time) over (partition by client_mac) as total_device_dwell_time -- this is relevant in evaluating temporospatial matches
      , count(*) over (partition by client_mac) as total_device_sightings -- this is relevant in evaluating temporospatial matches
  from {{ref('mart_merged_presence_pos__pos_sightings')}} s
      , lateral flatten( input => client_mac_info ) c
      , {{ seed_or_ref( ref('stg_business_profiles__d_business_geocode'), 'seed_d_business_geocode') }} g

  where s.business_id = g.business_id
)
-- pull all email/phone numbers associated with the client_mac
, devices_ as (
  select 
      upper(replace(ad.mac, ':')) as client_mac
      , clean_contact(email) as contact
      , count(*) over (partition by client_mac) as n_contacts_per_device

  from {{ seed_or_ref( ref('stg_crm__portal_accessdevice') , 'seed_portal_accessdevice') }} ad
       , {{ seed_or_ref( ref('stg_crm__portal_accessdeviceownership') , 'seed_portal_accessdeviceownership') }} ado
       , {{ seed_or_ref( ref('stg_crm__portal_userprofile') , 'seed_portal_userprofile') }} u

  where ad.accessdevice_id = ado.accessdevice_id
        and ado.userprofile_id = u.userprofile_id
        -- this was a period in access_device_ownership where something terrible happened
        -- the # entries exploded. It may be that was when we decided to merge email addresses across devices
        -- that shared a common email
        -- e.g Device A - a@b.com, cisaacson@zenreach.com, carrie@gmail.com
        --     Device B - a@b.com, kai@zenreach.com, kai@gmail.com
        --     became Device A - a@b.com, cisaacson@zenreach.com, carrie@gmail.com, kai@zenreach.com, kai@gmail.com
        --        and Device B - a@b.com, kai@zenreach.com, kai@gmail.com
        and ado.created not between '2016-07-09' and '2016-08-13' 
        and (contact like '%@%' or len(contact) = 11) -- email or phone
)
-- 
, sightings_contact_details_ as (
  select 
    s.sighting_id
    , s.business_id
    , s.classification
    , s.start_time_utc
    , s.end_time_utc
    , s.start_time
    , s.end_time
    , s.dwell_time
    , s.max_rssi
    , s.client_mac_info
    , s.client_mac
    , s.total_device_dwell_time
    , s.total_device_sightings
    , s.sighting_contact as contact
    , demo1.fullname
    , demo1.email_is_valid
    , demo1.email_score
    , d.contact as contact_alt
    , demo2.fullname as fullname_alt
    , demo2.email_is_valid as email_is_valid_alt
    , demo2.email_score as email_score_alt
  
    , dense_rank() over (partition by sighting_id order by ifnull(demo2.email_is_valid,FALSE) desc, ifnull(demo2.email_score,0) desc, d.contact) rank
    , row_number() over (partition by sighting_id, d.contact order by ifnull(demo2.email_is_valid,FALSE) desc, ifnull(demo2.email_score,0) desc) dupe
    , count(*) over (partition by s.sighting_id) n
    , array_size(s.client_mac_info) n_devices
  
  from sightings_ s
  -- append alternative email addresses
  left join devices_ d 
    on s.client_mac = d.client_mac
    -- don't consider email already assigned to the sighting as a potential alt email
    and s.sighting_contact <> d.contact
    -- there are some cases where there are dozens or even hundreds of emails per device.
    -- in sume cases these seem suspect, as though many devices are using the same client mac
    -- in these casess I'd rather not include an alternate contact / email and stick
    -- with the sighting selected at the location
    and d.n_contacts_per_device < 10
  left join {{ref('mart_merged_presence_pos__contact_details')}} demo1 on demo1.contact = s.sighting_contact
  -- join contact details for alternative email addresses
  left join {{ref('mart_merged_presence_pos__contact_details')}} demo2 on demo2.contact = d.contact 
)
, enriched_sightings_ as (
  select 
    s1.sighting_id
    , s1.business_id
    , s1.client_mac_info
    , s1.client_mac
    , s1.classification
    , s1.start_time_utc
    , s1.end_time_utc
    , s1.start_time
    , s1.end_time
    , s1.dwell_time
    , s1.max_rssi
    , s1.total_device_dwell_time
    , s1.total_device_sightings
    , s1.contact
    , s1.fullname
    , s1.email_score
    , s1.contact_alt as contact_alt_1
    , s1.fullname_alt as fullname_alt_1
    , s1.email_score_alt as email_score_alt_1
    , s2.contact_alt as contact_alt_2
    , s2.fullname_alt as fullname_alt_2
    , s2.email_score_alt as email_score_alt_2
    , s1.rank as rank_s1
    , s1.dupe as dupe_s1
    , s2.rank as rank_s2
    , s2.dupe as dupe_s2
  from sightings_contact_details_ s1 
  left join sightings_contact_details_ s2 on s1.sighting_id = s2.sighting_id and s1.client_mac = s2.client_mac and s2.rank = 2 and s2.dupe = 1 and s2.contact_alt <> s1.contact_alt
  where s1.rank = 1
)
, final_ as (
    select
        sighting_id
        , business_id
        , client_mac_info
        , classification
        , start_time_utc
        , end_time_utc
        , start_time
        , end_time
        , dwell_time
        , max_rssi
        , contact
        , fullname
        , email_score
        -- there's an edge case scenario that can happen when there are different alt-emails across multiple client_macs
        -- here we just pick one.
        , max(contact_alt_1) as contact_alt_1
        , max(fullname_alt_1) as fullname_alt_1
        , max(email_score_alt_1) as email_score_alt_1
        , max(contact_alt_2) as contact_alt_2
        , max(fullname_alt_2) as fullname_alt_2
        , max(email_score_alt_2) as email_score_alt_2
        , sum(total_device_dwell_time) as total_device_dwell_time
        , sum(total_device_sightings) as total_device_sightings
        , listagg(distinct client_mac, ', ')  within group (ORDER BY client_mac)  as client_macs
        , count(*) n_devices
    from enriched_sightings_
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
select *, current_timestamp() as created_at
from final_
