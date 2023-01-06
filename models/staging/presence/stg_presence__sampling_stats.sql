{{
    config(
        materialized='incremental',
        unique_key='INCREMENTAL_ID'
    )
}}

--------------------------------------------------------------------------------------------
-- PRESENCE SAMPLING STATS
-- Replacement of ads_campaigns.presence_sampling_stats
-- Incremental load of presence.presence_sampling_stats from presence.finished_sightings
-- The table was created by a upstream Local Ads service and decommissioned on 10/29/2019
-- This table is used for calculating Recognition Rates and is thusly, being revived
-- as a daily summary table to be able to able to analyze variability of RRs for a business over time
--------------------------------------------------------------------------------------------

SELECT
       BUSINESS_ID || '|' || TO_CHAR(date_trunc(HOUR, current_timestamp)::TIMESTAMP_NTZ, 'YYYY-MM-DD:HH24') as INCREMENTAL_ID,
       BUSINESS_ID
     , date_trunc(HOUR, START_TIME)                                                                            AS REPORT_DATETIME
     , SUM(iff(
            CLASSIFICATION = 'WALKIN'
            AND KNOWN_TO_ZENREACH
            AND NOT (KNOWN_TO_MERCHANT_ACCOUNT)
            AND NOT (KNOWN_TO_MERCHANT_LOCATION)
         , 1, 0))                                                                                             AS WALKIN_NETWORK
     , SUM(iff(
            CLASSIFICATION = 'WALKIN'
            AND (KNOWN_TO_MERCHANT_ACCOUNT OR KNOWN_TO_MERCHANT_LOCATION)
         , 1, 0))                                                                                             AS WALKIN_MERCHANT
     , SUM(iff(
             CLASSIFICATION = 'WALKIN'
             AND NOT (KNOWN_TO_ZENREACH)
         , 1, 0))                                   AS WALKIN_UNIDENTIFIED
     , SUM(
         iff(
             CLASSIFICATION = 'WALKBY'
             AND KNOWN_TO_ZENREACH
             AND NOT (KNOWN_TO_MERCHANT_ACCOUNT)
             AND NOT (KNOWN_TO_MERCHANT_LOCATION)
             , 1, 0))                                                                                             AS WALKBY_NETWORK
     , SUM(
         iff(
             CLASSIFICATION = 'WALKBY'
             AND (KNOWN_TO_MERCHANT_ACCOUNT OR KNOWN_TO_MERCHANT_LOCATION)
             , 1, 0))                                                                                             AS WALKBY_MERCHANT
     , SUM(iff(CLASSIFICATION = 'WALKBY' AND NOT (KNOWN_TO_ZENREACH), 1, 0))                                   AS WALKBY_UNIDENTIFIED
     , SUM(iff(CLASSIFICATION = 'NOTHUMAN', 1, 0))                                                             AS NOT_HUMAN -- New Classification since presence_sampling_stats was created
     , MIN(START_TIME)                                                                                         AS CREATED
     , MAX(END_TIME)                                                                                           AS UPDATED
     , current_date                                                                                            AS ASOF_DATE
FROM {{ ref('stg_presence__finished_sightings') }}
{% if is_incremental() %}
      -- this filter will only be applied on an incremental run
    WHERE   end_time > (select max(updated) from {{ this }})
    AND end_time < (select date_trunc(hour,current_timestamp()))
{% endif %}
group by 1, 2, 3
