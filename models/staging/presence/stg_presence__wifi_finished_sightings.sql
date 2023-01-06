{% if source('PRESENCE', 'WIFI_FINISHED_SIGHTINGS').is_table %}
{{
  config(
    materialized='view',
    pre_hook=[
      "
        delete from {{ source('PRESENCE', 'WIFI_FINISHED_SIGHTINGS') }}
        where datediff(day,to_timestamp_ntz(end_time), to_timestamp_ntz(current_date))< 30; "
    ]
  )
}}
{% endif %}

select *
from {{ source('PRESENCE', 'WIFI_FINISHED_SIGHTINGS') }}
