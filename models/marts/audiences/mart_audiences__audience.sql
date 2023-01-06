{{
    config(
        materialized='incremental',
        unique_key='INCREMENTAL_ID'
    )
}}

WITH user_business_ as (
SELECT (business_id || '|' || customer_sk) as INCREMENTAL_ID,
       customer_sk,
       business_id,
       FIRST_VALUE(created) over (partition by business_id, customer_sk order by created)::timestamp_ntz as created,
       LAST_VALUE(created) over (partition by business_id, customer_sk order by created)::timestamp_ntz  as updated,
        -- pick the most recent customer type for any business as baseline
       LAST_VALUE(customer_type) over (partition by business_id, customer_sk order by created) as customer_type
FROM {{ ref('stg_crm__analytics_customer') }}
WHERE (non_employee is Null or non_employee = True)
{% if is_incremental() %}
      -- this filter will only be applied on an incremental run
    and created > (select dateadd(day, -5, to_timestamp_ntz(max(created))) from {{ this }} where customer_type <> 'PASSIVE_DETECTION')
{% endif %}
),

user_sightings_ as(
SELECT (business_id || '|' || customer_sk) as INCREMENTAL_ID,
       customer_sk,
       business_id,
       created,
       current_date()      as updated,
       'PASSIVE_DETECTION' as customer_type
FROM {{ ref('mart_audiences__user_sightings') }} s
{% if is_incremental() %}
      -- this filter will only be applied on an incremental run
    WHERE created > (select dateadd(day, -5, to_timestamp_ntz(max(created))) from {{ this }} where customer_type = 'PASSIVE_DETECTION')
    and created <= (select to_timestamp_ntz(dateadd(day,-1,to_timestamp_ntz(current_date()))))
    and not exists (select 'x' from user_business_ b where s.business_id = b.business_id and s.customer_sk = b.customer_sk)
{% endif %}
)

SELECT * from user_business_

UNION ALL

SELECT * from user_sightings_
order by business_id, customer_sk
