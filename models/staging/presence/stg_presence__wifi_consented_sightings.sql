select *
from {{ source('PRESENCE', 'WIFI_CONSENTED_SIGHTINGS') }}
