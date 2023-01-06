{{ config(materialized='incremental') }}

SELECT *
FROM {{ source('AMS', 'SUBSCRIPTIONS_V2') }}
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where ASOF_DATE not in (select distinct ASOF_DATE from {{ this }})
{% endif %}