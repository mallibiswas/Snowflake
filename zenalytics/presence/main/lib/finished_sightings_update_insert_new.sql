------------------------------------------------------------------------------------------------------------------
---------  create table presence.finished_sightings --------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------
-- Change Log:
-- 3/14 [MB} lowercase contact id and location id to match keys in product domain
-- 4/24 [MB] add business_id to maintain (parent id, business id) relationship in portal_businessprofile
--           add parent_id to maintain (parent id, business id) relationship in portal_businessprofile
--           trim contact method string to standardize methods to email, facebook, etc.
--           renamed variables *_UNIX_TS to *_END_TS since we are not using UNIX/EPOCH time in raw sightings anymore
--           set contact_method to uppercase and standardized names based on the latest pipeline
--           added column portal_blip_count
--           excluded QoS email insightstatqos@example.com
-------------------------------------------------------------------------------------------------------------------


ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;
ALTER SESSION SET TIMEZONE = 'UTC';

use warehouse &{whname};
use database &{tgtdbname};
use role &{rolename};

SET MIN_END_TS = (select max(end_time) from &{tgtdbname}.&{tgtschemaname}.&{tgttablename});

SET MAX_END_TS = (select to_timestamp_ntz(current_date()));

-- log time stamp range
SELECT concat('Inserting from ts: $MIN_END_TS: ',$MIN_END_TS,' TO : $MAX_END_TS: ',$MAX_END_TS);

-- insert new finished sightings
INSERT INTO &{tgtdbname}.&{tgtschemaname}.FINISHED_SIGHTINGS
(	sighting_id
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
    , ASOF_DATE )
    WITH sightings_ AS (
        SELECT
            c.sighting_id
            , REPLACE(c.classification, 'Classification_') AS classification
            , c.start_time
            , c.end_time
            , c.blip_count
            , c.max_rssi
            , c.min_rssi
            , c.avg_rssi
            , DATEDIFF(second, c.start_time, c.end_time) AS dwell_time
            , c.anonymous_client_mac_info
            , CASE WHEN c.contact_id IS NOT NULL THEN f.client_mac_info ELSE NULL END AS client_mac_info
            , lower(c.contact_id) as contact_id
            , c.contact_info
            , REPLACE(c.contact_method, 'CONTACT_METHOD_') AS contact_method
            , lower(c.location_id) as location_id
            , lower(c.location_id) as business_id
            , lower(c.account_id) as account_id
            , lower(c.account_id) as parent_id
            , c.known_to_zenreach
            , c.known_to_merchant_account
            , c.known_to_merchant_location
            , c.privacy_version
            , c.terms_version
            , c.bundle_version
            , c.is_employee
            , c.portal_blip_count
            , ROW_NUMBER() OVER (PARTITION BY c.sighting_id
                                 ORDER BY DATEADD(seconds, RANDOM(1), c.end_time)) AS dupe_rank
        FROM &{srcdbname}.&{srcschemaname}.wifi_consented_sightings c
        LEFT JOIN &{srcdbname}.&{srcschemaname}.wifi_finished_sightings f
        ON c.sighting_id = f.sighting_id
        WHERE c.end_time > $MIN_END_TS AND c.end_time <= $MAX_END_TS
        AND IFNULL(c.contact_info, '') NOT LIKE '%qos.zenreach.com'
        AND IFNULL(c.contact_info, '') <> 'insightstatqos@example.com'
      )
SELECT
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
;
