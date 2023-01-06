{{
    config(
        materialized='incremental'
    )
}}

WITH CTE_PORTAL_BLIP AS (
    SELECT B.SENSOR_MAC,
           MAX(TO_TIMESTAMP(B.SERVER_TIME)) AS LAST_TS,
           C.REPORT_DATE_SK,
           C.REPORT_DATE
    FROM {{ ref('stg_presence__portal_blips') }} B
             INNER JOIN {{ ref('mart_bizint__d_date') }} C
                        ON C.REPORT_DATE = TO_DATE(TO_TIMESTAMP(B.SERVER_TIME))
    WHERE C.REPORT_DATE = current_date
    GROUP BY C.REPORT_DATE_SK, C.REPORT_DATE, B.SENSOR_MAC
),
     CTE_LOCATION_BLIP AS (
         SELECT B.SENSOR_MAC,
                MAX(TO_TIMESTAMP(B.SERVER_TIME)) AS LAST_TS,
                C.REPORT_DATE_SK,
                C.REPORT_DATE
         FROM {{ ref('stg_presence__location_blips') }} B
                  INNER JOIN {{ ref('mart_bizint__d_date') }} C
                             ON C.REPORT_DATE = TO_DATE(TO_TIMESTAMP(B.SERVER_TIME))
         WHERE C.REPORT_DATE = current_date
         GROUP BY C.REPORT_DATE_SK, C.REPORT_DATE, B.SENSOR_MAC
     )
SELECT L.SENSOR_MAC                      AS LOCATION_SENSOR_MAC,
       P.SENSOR_MAC                      AS PORTAL_SENSOR_MAC,
       P.LAST_TS                         AS LAST_PORTAL_BLIP_TS,
       L.LAST_TS                         AS LAST_LOCATION_BLIP_TS,
       NVL(L.REPORT_DATE, P.REPORT_DATE) AS ASOF_DATE
FROM CTE_PORTAL_BLIP P
         FULL OUTER JOIN CTE_LOCATION_BLIP L
                         ON P.REPORT_DATE_SK = L.REPORT_DATE_SK
                             AND P.SENSOR_MAC = L.SENSOR_MAC
{% if is_incremental() %}
    WHERE ASOF_DATE NOT IN (SELECT DISTINCT ASOF_DATE FROM {{ this }})
{% endif %}