{{
    config(
        materialized='incremental',
        unique_key='sighting_id',
        
        post_hook=[
          "
            delete from {{ this }} target
            using {{ source('PRIVACY', 'PRIVACY_REQUEST') }} source
            where target.email = source.contact_info;
            "
        ]
    )
}}

WITH cte_new_sightings AS (
    SELECT (location_id || '|' || contact_info) as sighting_id
    FROM {{ source('PRESENCE', 'WIFI_CONSENTED_SIGHTINGS') }}
    WHERE classification = 'Classification_WALKIN'
      and contact_method = 'CONTACT_METHOD_EMAIL'
      AND contact_info IS NOT NULL
      and is_employee = false
      and end_time >= to_timestamp_ntz(dateadd(hours, -24, current_timestamp()))
    GROUP BY (location_id, contact_info)
),
     cte_updated_sightings AS (
         SELECT (location_id || '|' || contact_info) as sighting_id,
                location_id     as business_id,
                contact_info    as email,
                count(*)        as visit_count,
                min(start_time) as first_seen,
                max(end_time)   as last_seen
         FROM {{ source('PRESENCE', 'WIFI_CONSENTED_SIGHTINGS') }} b
                  INNER JOIN cte_new_sightings a ON (
                 a.sighting_id = (location_id || '|' || contact_info)
             )
             group by a.sighting_id,b.location_id,b.contact_info
     )
SELECT sighting_id,
       business_id,
       email,
       visit_count,
       first_seen,
       last_seen
FROM cte_updated_sightings
