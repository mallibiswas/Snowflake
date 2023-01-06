{{
    config(
        materialized='incremental',
        unique_key='SENSOR_MAC'
    )
}}

SELECT B.SENSOR_MAC,
       MAX(TO_TIMESTAMP(B.SERVER_TIME)) AS LAST_TS,
       current_date as ASOF_DATE
FROM {{ ref('stg_presence__portal_blips') }} b
{% if is_incremental() %}
WHERE TO_TIMESTAMP(B.SERVER_TIME) >= DATEADD(DAY, -2, (select MAX(LAST_TS) FROM {{ this }}))
{% endif %}
GROUP BY b.SENSOR_MAC