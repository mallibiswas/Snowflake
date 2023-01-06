{% if source('PRESENCE', 'PORTAL_BLIPS').is_table %}
    {{
      config(
        materialized='view',
        pre_hook=[
          "
            delete from {{ source('PRESENCE', 'PORTAL_BLIPS') }}
            where server_time < (select DATE_PART(EPOCH_SECOND, to_timestamp_ntz(dateadd(day, -30, current_date()))));
            "
        ]
      )
    }}
{% endif %}

select *
from {{ source('PRESENCE', 'PORTAL_BLIPS') }}
