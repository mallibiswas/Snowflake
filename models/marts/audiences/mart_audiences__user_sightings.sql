{{
    config(
        materialized='incremental',
        unique_key='incremental_id'
    )
}}

with cte_new_sighting AS (
  SELECT  (business_id || '|' || contact_id) AS incremental_id
  FROM {{ ref('stg_presence__finished_sightings') }}
  WHERE classification = 'WALKIN'
    AND contact_info like '%@%'
{% if is_incremental() %}
    AND end_time>( SELECT  dateadd(day,-5,to_timestamp_ntz(MAX(created)))
-- this filter will only be applied on an incremental run
    FROM {{ this }}) AND contact_id is not null AND end_time <= (SELECT  to_timestamp_ntz(dateadd(day,-1,to_timestamp_ntz(current_date()))))
{% endif %}
    AND ifnull(is_employee, FALSE) = FALSE
GROUP BY  business_id
         ,contact_id
         ,contact_info
         ,sighting_id
),

cte_updated_sightings AS (
  SELECT  (business_id || '|' || contact_id)  AS INCREMENTAL_ID
          ,business_id                        AS BUSINESS_ID
          ,contact_id                         AS CONTACT_ID
          ,substr(sha1(contact_info),1,24)    AS CUSTOMER_SK
          ,COUNT(distinct sighting_id)        AS SIGHTINGS
          ,MIN(start_time)                    AS FIRST_SIGHTED
          ,MAX(end_time)                      AS LAST_SIGHTED
          ,FIRST_SIGHTED                      AS CREATED
          ,LAST_SIGHTED                       AS UPDATED
          ,current_date()                     AS ASOF_DATE
FROM {{ ref('stg_presence__finished_sightings') }} b
          INNER JOIN cte_new_sighting a
          ON ( a.incremental_id = (business_id || '|' || contact_id)
          )
GROUP BY  b.business_id
         ,b.contact_id
         ,b.contact_info
         ,a.incremental_id
         )

SELECT a.incremental_id,
       a.business_id,
       a.contact_id,
       a.customer_sk,
       a.sightings,
       a.first_sighted,
       a.last_sighted,
       a.created,
       a.updated,
       a.asof_date
FROM cte_updated_sightings a
{% if is_incremental() %}
-- this will only be applied on an incremental run
    INNER JOIN {{this}} b on (a.incremental_id=b.incremental_id and b.LAST_SIGHTED<a.FIRST_SIGHTED)
{% endif %}

