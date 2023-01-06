with sightings_ as (
  select
      location_id as business_id
      , contact_id
      , start_time
      , end_time
      , datediff(milliseconds, start_time, end_time)/60000 as dwell_time_minutes
      , max_rssi
      , replace(classification, 'Classification_') as class
      , known_to_merchant_account
      , known_to_zenreach
      -- local date/time
      , convert_timezone('UTC', nvl(google_timezone,'America/Los_Angeles'), start_time) as local_time -- pick west coast if TZ is missing
      , date_trunc(day,convert_timezone('UTC', nvl(google_timezone,'America/Los_Angeles'), start_time))::date as local_date -- pick west coast if TZ is missing
      -- walkins
      , case when classification = 'Classification_WALKIN' and KNOWN_TO_MERCHANT_ACCOUNT = TRUE then 1 else 0 end as merchant_walkin
      , case when classification = 'Classification_WALKIN' and KNOWN_TO_ZENREACH = TRUE then 1 else 0 end as network_walkin
      , case when classification = 'Classification_WALKIN' and contact_id is null then 1 else 0 end as unidentified_walkin
      , case when classification = 'Classification_WALKIN' then 1 else 0 end as total_walkin
      -- walkbys
      , case when classification = 'Classification_WALKBY' and KNOWN_TO_MERCHANT_ACCOUNT = TRUE then 1 else 0 end as merchant_walkby
      , case when classification = 'Classification_WALKBY' and KNOWN_TO_ZENREACH = TRUE then 1 else 0 end as network_walkby
      , case when classification = 'Classification_WALKBY' and contact_id is null then 1 else 0 end as unidentified_walkby
      , case when classification = 'Classification_WALKBY' then 1 else 0 end as total_walkby
      -- visits
      , case when classification = 'Classification_WALKIN' and KNOWN_TO_MERCHANT_ACCOUNT = TRUE then 1 else 0 end as visit
      , current_timestamp as asof_date_utc
  from {{ source('PRESENCE', 'WIFI_CONSENTED_SIGHTINGS') }} s
  left join {{ ref('stg_business_features__location') }} bf on s.location_id = bf.business_id
  where end_time > dateadd(days, -60, current_date())
  and is_employee = FALSE
)
select  business_id,
        local_date,
        dwell_time_minutes,
        ROUND(dwell_time_minutes/60,1) as dwell_time_hours,
        hour(local_time) as hour_local_time,
        width_bucket(NVL(max_rssi,0),0,-100,20) as max_rssi_bucket,
        -- walkby
        sum(network_walkin) as network_walkins,
        sum(merchant_walkin) as merchant_walkins,
        sum(unidentified_walkin) as unidentified_walkins,
        sum(total_walkin) as total_walkins,
        -- walkin
        sum(network_walkby) as network_walkbys,
        sum(merchant_walkby) as merchant_walkbys,
        sum(unidentified_walkby) as unidentified_walkbys,
        sum(total_walkby) as total_walkbys,
        -- visit
        sum(visit) as total_visits
from    sightings_
group by 1,2,3,4,5,6