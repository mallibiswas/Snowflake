-----------------------------------------------------------------------------------------------
-------------- CREATE CURRENT COPY PORTAL_BUSINESSPROFILE -------------------------------------
-----------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE ZENSAND.crm.portal_businessprofile
AS
select *
from zenprod.crm.portal_businessprofile;

-----------------------------------------------------------------------------------------------
-------------- CREATE EMPTY PRIVACY_REQUEST WITH CORRECT STRUCTURE -------------------------
-----------------------------------------------------------------------------------------------

CREATE SCHEMA ZENSAND.privacy;

CREATE OR REPLACE TABLE ZENSAND.privacy.privacy_request
AS
select *
from zenprod.privacy.privacy_request
limit 0;

-----------------------------------------------------------------------------------------------
-------------- CREATE EMPTY FINISHED_SIGHTINGS WITH CORRECT STRUCTURE -------------------------
-----------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE ZENSAND.presence.finished_sightings
AS
WITH sighting_old_ AS (
  SELECT 
      '' AS sighting_id 
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
  LIMIT 0
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
        , 1 AS dupe_rank
    FROM zenprod.presence.wifi_consented_sightings c
    LEFT JOIN zenprod.presence.wifi_finished_sightings f
        ON c.sighting_id = f.sighting_id
    LIMIT 0
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
FROM sighting_new_);

------------------------------------------------------------------------
---------------------- INSERT TARGET SIGHTINGS -------------------------
------------------------------------------------------------------------

insert into ZENSAND.presence.finished_sightings (sighting_id
                                                         , classification
                                                         , start_time
                                                         , end_time
                                                         , anonymous_client_mac_info
                                                         , client_mac_info
                                                         , contact_id
                                                         , contact_info
                                                         , location_id
                                                         , account_id
                                                         , known_to_zenreach
                                                         , known_to_merchant_account
                                                         , known_to_merchant_location
                                                )
  select 
     column1
     , column2
     , column3
     , column4
     , parse_json(column5)
     , parse_json(column6)
     , column7
     , column8
     , column9
     , column10
     , column11
     , column12
     , column13
  from values
  (
    '1_merchant_delete_different_account_known_to_zenreach_should_not_change'
    , 'WALKBY'
    , TO_TIMESTAMP_NTZ(current_timestamp())
    , TO_TIMESTAMP_NTZ(current_timestamp())
    , '[{"client_mac_anonymization": "865A4CAD1BFD3FAE95B7F8C9543094FF", "vendor_prefix": "C09AD0"}]'
    , '[{"client_mac": "C09AD083E5D2", "client_mac_anonymization": "865A4CAD1BFD3FAE95B7F8C9543094FF"}]'
    , '111111111111111111111111'
    , 'merchant_delete_different_account_known_to_zenreach@test_1.com'
    , '54063bf61081cd2c2553de0a' -- test location id -> Kai's House
    , '54bffeb41081cd61795d1666' -- test account id
    , TRUE
    , FALSE
    , FALSE
  )
  ,
  (
    '2_merchant_delete_on_this_account_should_set_known_to_merchant_account_to_false'
    , 'WALKBY'
    , TO_TIMESTAMP_NTZ(current_timestamp())
    , TO_TIMESTAMP_NTZ(current_timestamp())
    , '[ { "client_mac_anonymization": "865A4CAD1BFD3FAE95B7F8C9543094FF", "vendor_prefix": "C09AD0" } ]'
    , '[ { "client_mac": "C09AD083E5D2", "client_mac_anonymization": "865A4CAD1BFD3FAE95B7F8C9543094FF" } ]'
    , '111111111111111111111112'
    , 'merchant_delete_on_this_account@test_2.com'
    , '54063bf61081cd2c2553de0a' -- test location id -> Kai's House
    , '54bffeb41081cd61795d1666' -- test account id
    , TRUE
    , TRUE
    , FALSE
  )
  ,
  (
    '3_merchant_delete_this_account_should_set_known_to_merchant_location_and_account_to_false'
    , 'WALKIN'
    , TO_TIMESTAMP_NTZ(current_timestamp())
    , TO_TIMESTAMP_NTZ(current_timestamp())
    , '[ { "client_mac_anonymization": "865A4CAD1BFD3FAE95B7F8C9543094FF", "vendor_prefix": "C09AD0" } ]'
    , '[ { "client_mac": "C09AD083E5D2", "client_mac_anonymization": "865A4CAD1BFD3FAE95B7F8C9543094FF" } ]'
    , '111111111111111111111112'
    , 'merchant_delete_on_this_account@test_2.com'
    , '5407525c1081cd2c9e72ba89' -- test location id -> Thomas' House
    , '54bffeb41081cd61795d1666' -- test account id
    , TRUE
    , TRUE
    , TRUE
  )
  ,
  (
    '4_global_delete_user_part_1'
    , 'WALKIN'
    , TO_TIMESTAMP_NTZ(current_timestamp())
    , TO_TIMESTAMP_NTZ(current_timestamp())
    , '[ { "client_mac_anonymization": "865A4CAD1BFD3FAE95B7F8C9543094FF", "vendor_prefix": "C09AD0" } ]'
    , '[ { "client_mac": "C09AD083E5D2", "client_mac_anonymization": "865A4CAD1BFD3FAE95B7F8C9543094FF" } ]'
    , '111111111111111111111113'
    , 'global_delete_user@test_4.com'
    , '5d40a3786e032800017e764d' -- Zenreach QoS Testing Location 2
    , '5d40a3622a10f80001e12392' -- Zenreach QoS Testing Account
    , TRUE
    , TRUE
    , TRUE
  )
    ,
  (
    '5_global_delete_user_part_2'
    , 'WALKIN'
    , TO_TIMESTAMP_NTZ(current_timestamp())
    , TO_TIMESTAMP_NTZ(current_timestamp())
    , '[ { "client_mac_anonymization": "865A4CAD1BFD3FAE95B7F8C9543094FF", "vendor_prefix": "C09AD0" } ]'
    , '[ { "client_mac": "C09AD083E5D2", "client_mac_anonymization": "865A4CAD1BFD3FAE95B7F8C9543094FF" } ]'
    , '111111111111111111111113'
    , 'global_delete_user@test_4.com'
    , '5d40a36e6e032800017e7649' -- Zenreach QoS Testing Location 1
    , '5d40a3622a10f80001e12392' -- Zenreach QoS Testing Account
    , TRUE
    , TRUE
    , FALSE
  )
  ,
  (
    '6_innocent_bystander_at_QoS_Testing_loc_should_not_change'
    , 'WALKIN'
    , TO_TIMESTAMP_NTZ(current_timestamp())
    , TO_TIMESTAMP_NTZ(current_timestamp())
    , '[ { "client_mac_anonymization": "865A4CAD1BFD3FAE95B7F8C9543094FF", "vendor_prefix": "C09AD0" } ]'
    , '[ { "client_mac": "C09AD083E5D2", "client_mac_anonymization": "865A4CAD1BFD3FAE95B7F8C9543094FF" } ]'
    , '111111111111111111111114'
    , 'innocent_bystander@test_4.com'
    , '5407525c1081cd2c9e72ba89' -- Zenreach QoS Testing Location 1
    , '54bffeb41081cd61795d1666' -- Zenreach QoS Testing Account
    , TRUE
    , FALSE
    , FALSE
  )
 ;

------------------------------------------------------------------------
---------------------- INSERT PRIVACY REQUESTS -------------------------
------------------------------------------------------------------------

insert into ZENSAND.privacy.privacy_request (request_id
                                                    , root_business_id
                                                    , contact_info
                                                    , request_type
                                                    , created)
values
(
  '1_merchant_delete_different_account_known_to_zenreach_should_not_change'
 , '5d40a3622a10f80001e12392' -- Zenreach QoS Testing Account
 , 'merchant_delete_different_account_known_to_zenreach@test_1.com'
 , 'REQUEST_TYPE_ERASURE'
 , current_timestamp()
),
(
  '2-3_merchant_delete_known_to_merchant_account'
 , '54bffeb41081cd61795d1666' -- Zenreach QoS Testing Account
 , 'merchant_delete_on_this_account@test_2.com'
 , 'REQUEST_TYPE_ERASURE'
 , current_timestamp()
),
(
  '4-5-global_delete_user'
 , '' -- global delete
 , 'global_delete_user@test_4.com'
 , 'REQUEST_TYPE_ERASURE'
 , current_timestamp()
);

-----------------------------------------------------------------------------------------------
--------------------------------- BEFORE!            ------------------------------------------
-----------------------------------------------------------------------------------------------

-- 6 sightings
select *
from ZENSAND.presence.finished_sightings
order by 1

-----------------------------------------------------------------------------------------------
--------------------------------- RUN PRIVACY DELETES -----------------------------------------
-----------------------------------------------------------------------------------------------

create or replace table ZENSAND.privacy.privacy_deletes as 
with location_accounts_ as (
  select distinct
        coalesce(parent_id, business_id) as root_business_id
        , business_id
    from ZENSAND.crm.portal_businessprofile
)
, privacy_request_ as (
  select *
  from ZENSAND.privacy.privacy_request p 
  where request_type = 'REQUEST_TYPE_ERASURE'
        and created >= dateadd(day,-8,current_date() )  
)
, privacy_request_global_ as (
  select distinct NULL as business_id, contact_info, TRUE as is_global
  from privacy_request_
  where ifnull(root_business_id, '') = ''
)
, privacy_request_merchant_ as (
  select business_id, contact_info, FALSE as is_global
  from privacy_request_ p
  left join location_accounts_ a
    on p.root_business_id = a.root_business_id
  where ifnull(p.root_business_id, '') <> ''
)
select business_id, contact_info, is_global from privacy_request_global_
union
select business_id, contact_info, is_global from privacy_request_merchant_ where contact_info not in (select contact_info from privacy_request_global_);

-- Honouring MERCHANT privacy deletes
-- [Mark G.] When someone asks to not be tracked at a business anymore, we just need to update in_business_network to false to indicate that only ZENREACH should be able to see this information 
-- (unless ZENREACH delete has been requested as well)
-- [Carrie I.] updates to presence model in_business_network split between -> known_to_merchant_account and known_to_merchant_location.

-- Honouring ZENREACH privacy deletes
-- When someone asks to be forgotten by zenreach, we must either delete or remove all personal data from the sighting.  If you elect to anonymize (which I recommend), then the following fields must be set to null:
-- client mac, contact_info, contact_id, contact_method

update ZENSAND.presence.finished_sightings s
set s.known_to_merchant_account = FALSE -- merchant delete --> minimum deletion
  , s.known_to_merchant_location = FALSE -- merchant delete --> minimum deletion
  , s.known_to_zenreach = case when is_global = TRUE then FALSE else s.known_to_zenreach end -- global (known_to_zenreach should always = TRUE if contact_info is not NULL)
  , s.client_mac_info = case when is_global = TRUE then NULL else s.client_mac_info end -- global
  , s.contact_info = case when is_global = TRUE then NULL else s.contact_info end -- global
    , s.contact_id = case when is_global = TRUE then NULL else s.contact_id end -- global
    , s.contact_method = case when is_global = TRUE then NULL else s.contact_method end -- global
from ZENSAND.privacy.privacy_deletes pr
where pr.contact_info = s.contact_info
    and (pr.business_id is NULL or pr.business_id = s.location_id);

-----------------------------------------------------------------------------------------------
--------------------------------- SEE WHAT HAPPENED! ------------------------------------------
-----------------------------------------------------------------------------------------------

-- should be 6 sightings
-- 1 -> merchant delete for this user but for another location. no change to identified sighting.
-- 2 -> known_to_merchant_account = FALSE, known_to_merchant_location = FALSE
-- 3 -> known_to_merchant_account = FALSE, known_to_merchant_location = FALSE
-- 4 -> global user delete -> known_to_* = FALSE client_mac_info = NULL, contact_info = NULL, contact_id = FALSE, contact_method = NULL
-- 5 -> global user delete -> known_to_* = FALSE client_mac_info = NULL, contact_info = NULL, contact_id = FALSE, contact_method = NULL
-- 6 -> innocent bystander at a location with merchant deletes. no change to identified sighting.

select *
from zensand.presence.finished_sightings
order by 1;

-----------------------------------------------------------------------------------------------
-------------- PUT PRIVACY_REQUEST BACK -------------------------------------------------------
-----------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE ZENSAND.privacy.privacy_request
AS
select *
from zenprod.privacy.privacy_request;