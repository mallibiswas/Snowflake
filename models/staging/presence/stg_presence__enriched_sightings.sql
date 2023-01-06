{% if source('PRESENCE', 'ENRICHED_SIGHTINGS').is_table %}
{{
  config(
    materialized='view'
  )
}}
{% endif %}

select *
from {{ source('PRESENCE', 'ENRICHED_SIGHTINGS') }}

