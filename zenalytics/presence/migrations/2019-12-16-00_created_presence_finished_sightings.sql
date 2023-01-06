CREATE OR REPLACE TABLE zenalytics.presence.finished_sightings
AS
WITH sighting_old_ AS (
  SELECT 
      row_number() over (ORDER BY start_time, end_time, business_id, client_mac_anonymized)::string AS sighting_id 
      , CASE WHEN ifnull(is_walk_in,FALSE) = TRUE THEN 'WALKIN' ELSE 'WALKBY' END AS classification
      , to_timestamp(start_time, 3) AS start_time
      , to_timestamp(end_time, 3) AS end_time
      , blip_count
      , max_rssi
      , min_rssi
      , avg_rssi
      , datediff(milliseconds, to_timestamp(start_time,3), to_timestamp(end_time,3))/1000 AS dwell_time
      , to_array(object_construct('client_mac_anonymization', client_mac_anonymized::string, 'vendor_prefix', LEFT(upper(REPLACE(client_mac,':')),6))) AS anonymous_client_mac_info
      , CASE WHEN contact_id IS NOT null THEN to_array(object_construct('client_mac_anonymization', client_mac_anonymized::string, 'client_mac', client_mac)) ELSE NULL END AS client_mac_info
      , contact_id
      , contact_info
      , contact_method
      , business_id AS location_id
      , NULL AS account_id
      , CASE WHEN contact_id IS NOT NULL THEN TRUE ELSE FALSE END AS known_to_zenreach
      , NULL AS known_to_merchant_account
      , CASE WHEN in_business_network = TRUE THEN TRUE ELSE FALSE END AS known_to_merchant_location
      , NULL AS privacy_version
      , NULL AS terms_version
      , NULL AS bundle_version
      , NULL AS is_employee
  FROM zenprod.presence.enriched_sightings
  WHERE 
      to_timestamp(end_time,3) < '2019-12-01 00:00:00.000'
      -- AND to_timestamp(end_time,3) > '2019-11-30 00:00:00.000' // COMMENT OUT - FOR TESTING ONLY
      AND STATUS = 'FINISHED'
)

, sighting_new_ AS (
    SELECT 
        c.sighting_id
        , REPLACE(c.classification, 'Classification_') AS classification
        , c.start_time
        , c.end_time
        , c.blip_count
        , c.max_rssi
        , c.min_rssi
        , c.avg_rssi
        , datediff(milliseconds, c.start_time, c.end_time)/1000 AS dwell_time
        , c.anonymous_client_mac_info
        , CASE WHEN c.contact_id IS NOT null THEN f.client_mac_info ELSE NULL END AS client_mac_info
        , c.contact_id
        , c.contact_info
        , c.contact_method
        , c.location_id
        , c.account_id
        , c.known_to_zenreach
        , c.known_to_merchant_account
        , c.known_to_merchant_location
        , c.privacy_version
        , c.terms_version
        , c.bundle_version
        , c.is_employee
        , row_number() over (PARTITION BY c.sighting_id
                             ORDER BY dateadd(seconds, random(1), c.end_time)) AS dupe_rank
    FROM zenprod.presence.wifi_consented_sightings c
    LEFT JOIN zenprod.presence.wifi_finished_sightings f
        ON c.sighting_id = f.sighting_id
    WHERE c.end_time > '2019-12-01 00:00:00.000'
        -- AND c.end_time < '2019-12-02 00:00:00.000' // COMMENT OUT - FOR TESTING ONLY
        AND ifnull(c.contact_info, '') NOT LIKE '%qos.zenreach.com'
    ORDER BY dupe_rank DESC
)

(SELECT 
     sighting_id
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
    , account_id
    , known_to_zenreach
    , known_to_merchant_account
    , known_to_merchant_location
    , privacy_version
    , terms_version
    , bundle_version
    , is_employee
    , CURRENT_TIMESTAMP() AS ASOF_DATE
FROM sighting_old_)
UNION
(SELECT 
    sighting_id
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
    , account_id
    , known_to_zenreach
    , known_to_merchant_account
    , known_to_merchant_location
    , privacy_version
    , terms_version
    , bundle_version
    , is_employee
    , CURRENT_TIMESTAMP() AS ASOF_DATE
FROM sighting_new_
WHERE dupe_rank = 1);