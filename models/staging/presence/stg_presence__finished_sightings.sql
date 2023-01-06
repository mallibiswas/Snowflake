{{
config(
    materialized='incremental',
    cluster_by=['start_time','sighting_id'],
    post_hook=[
      "
        update {{ this }}
        set    client_mac_info = NULL
        where  contact_id is null -- unconsented
            and    END_TIME >= (select to_timestamp_ntz(dateadd(day, -35, current_date()))) -- younger than 35 days
            and    END_TIME < (select to_timestamp_ntz(dateadd(day, -30, current_date()))); -- older than 30 days


        -- Honouring MERCHANT privacy deletes
        -- [Mark G.] When someone asks to not be tracked at a business anymore, we just need to update in_business_network to false to indicate that only ZENREACH should be able to see this information
        -- (unless ZENREACH delete has been requested as well)
        -- [Carrie I.] updates to presence model in_business_network split between -> known_to_merchant_account and known_to_merchant_location.

        -- Honouring ZENREACH privacy deletes
        -- When someone asks to be forgotten by zenreach, we must either delete or remove all personal data from the sighting.  If you elect to anonymize (which I recommend), then the following fields must be set to null:
        -- client mac, contact_info, contact_id, contact_method

        update {{ this }} s
        set s.known_to_merchant_account = FALSE -- merchant delete --> minimum deletion
            , s.known_to_merchant_location = FALSE -- merchant delete --> minimum deletion
            , s.known_to_zenreach = case when is_global = TRUE then FALSE else s.known_to_zenreach end -- global (known_to_zenreach should always = TRUE if contact_info is not NULL)
            , s.client_mac_info = case when is_global = TRUE then NULL else s.client_mac_info end -- global
            , s.contact_info = case when is_global = TRUE then NULL else s.contact_info end -- global
            , s.contact_id = case when is_global = TRUE then NULL else s.contact_id end -- global
            , s.contact_method = case when is_global = TRUE then NULL else s.contact_method end -- global
        from {{ ref('stg_privacy__privacy_deletes') }} pr
        where pr.contact_info = s.contact_info
              and (pr.business_id is NULL or pr.business_id = s.location_id);
        "
    ]
  )
}}

WITH sightings_ AS (
    SELECT c.sighting_id
         , REPLACE(c.classification, 'Classification_')                                                    AS classification
         , c.start_time
         , c.end_time
         , c.blip_count
         , c.max_rssi
         , c.min_rssi
         , c.avg_rssi
         , DATEDIFF(second, c.start_time, c.end_time)                                                      AS dwell_time
         , c.anonymous_client_mac_info
         , CASE WHEN c.contact_id IS NOT NULL THEN f.client_mac_info ELSE NULL END                         AS client_mac_info
         , lower(c.contact_id)                                                                             as contact_id
         , c.contact_info
         , REPLACE(c.contact_method, 'CONTACT_METHOD_')                                                    AS contact_method
         , lower(c.location_id)                                                                            as location_id
         , lower(c.location_id)                                                                            as business_id
         , lower(c.account_id)                                                                             as account_id
         , lower(c.account_id)                                                                             as parent_id
         , c.known_to_zenreach
         , c.known_to_merchant_account
         , c.known_to_merchant_location
         , c.privacy_version
         , c.terms_version
         , c.bundle_version
         , c.is_employee
         , c.portal_blip_count
         , ROW_NUMBER() OVER (PARTITION BY c.sighting_id ORDER BY DATEADD(seconds, RANDOM(1), c.end_time)) AS dupe_rank
    FROM {{ source('PRESENCE', 'WIFI_CONSENTED_SIGHTINGS') }} c
             LEFT JOIN {{ source('PRESENCE', 'WIFI_FINISHED_SIGHTINGS') }} f ON c.sighting_id = f.sighting_id
    WHERE IFNULL(c.contact_info, '') NOT LIKE '%qos.zenreach.com'
          AND IFNULL(c.contact_info, '') <> 'insightstatqos@example.com'
        {% if is_incremental() %}
          AND c.end_time > (SELECT MAX(end_time) from {{ this }})
        {% endif %}
)
SELECT sighting_id
     , classification
     , start_time
     , end_time
     , blip_count
     , max_rssi
     , min_rssi
     , avg_rssi
     , dwell_time
     , anonymous_client_mac_info
     , client_mac_info
     , contact_id
     , contact_info
     , contact_method
     , location_id
     , business_id
     , account_id
     , parent_id
     , known_to_zenreach
     , known_to_merchant_account
     , known_to_merchant_location
     , privacy_version
     , terms_version
     , bundle_version
     , is_employee
     , portal_blip_count
     , current_date() as ASOF_DATE
FROM sightings_
WHERE dupe_rank = 1

