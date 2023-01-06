with signal_strength_thresholds__ as (
  select distinct
      location_id as business_id
      , min_walkin_signal_strength
      , effective_as_of
      , lead(effective_as_of, 1, to_timestamp_ntz('3000-01-01')) over (partition by business_id order by effective_as_of) effective_until
      , row_number() over (partition by business_id order by effective_as_of) rank
  from {{ source('PRESENCE', 'LOCATION_CLASSIFIER_CONFIG_THRESHOLDS' ) }}
  where min_walkin_signal_strength is not null
)
, signal_strength_thresholds_ as (
  select
    business_id
    , min_walkin_signal_strength
    , effective_as_of
    , case when rank = 1 then '1970-01-01' else effective_as_of end as effective_from
    , effective_until
    , rank
  from signal_strength_thresholds__
)
, iot_ as (
  select
    mac_prefix
    , vendor_name
    , not_human
    , modified_at
    , row_number() over (partition by mac_prefix order by modified_at desc) rank
  from {{ source('PRESENCE', 'MAC_PREFIX_TO_VENDOR_MAPPING') }}
)
  select
  s.sighting_id,
  case
        when portal_blip_count > 0 then classification
        when i.not_human = TRUE and contact_id is null then 'NOTHUMAN'
        when max_rssi < ifnull(t.min_walkin_signal_strength,-90) then 'WALKBY'
        else classification
     end as classification
   , s.start_time
   , s.end_time
   , s.blip_count
   , s.max_rssi
   , s.min_rssi
   , s.avg_rssi
   , dwell_time
   , s.anonymous_client_mac_info
   , s.client_mac_info
   , s.contact_id
   , substr(sha1(contact_info),1,24) as customer_sk
   , s.contact_info
   , s.contact_method
   , s.location_id
   , s.business_id
   , s.account_id
   , s.parent_id
   , s.known_to_zenreach
   , s.known_to_merchant_account
   , s.known_to_merchant_location
   , s.privacy_version
   , s.terms_version
   , s.bundle_version
   , s.is_employee
   , s.portal_blip_count
   , i.not_human
   , i.vendor_name
   , t.min_walkin_signal_strength
   , case when lower(w.vendor_name) like '%apple%' then 'Apple'
          when lower(w.vendor_name) like '%murata%' then 'Apple'
          else 'Other' end as os
   , s.asof_date
  from {{ ref('stg_presence__finished_sightings') }} s
  left join signal_strength_thresholds_ t on s.business_id = t.business_id and s.end_time between t.effective_from and t.effective_until
  left join iot_ i on s.ANONYMOUS_CLIENT_MAC_INFO[0]:vendor_prefix = i.mac_prefix
  left join {{ source('PRESENCE', 'DEVICE_MANUFACTURER_WIRESHARK') }} w on s.ANONYMOUS_CLIENT_MAC_INFO[0]:vendor_prefix = w.ASSIGNMENT
  where end_time > '2019-01-01'