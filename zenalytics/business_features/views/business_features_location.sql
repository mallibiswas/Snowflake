-- BUSINESS_FEATUREBUSINESS_FEATURES_LOCATIONS_LOCATION VIEW
CREATE OR REPLACE VIEW ZENDEV.BUSINESS_CLASSIFICATION.BUSINESS_FEATURES_LOCATION
COMMENT ='business address features collected from 3rd party sources' AS
SELECT 
  business_id AS business_id
  , google:address AS google_address
  , google:lat AS google_lat
  , google:long AS google_long
  , google:timezone AS google_timezone
  , google:maps_url AS google_maps_url
FROM "ZENDEV"."BUSINESS_CLASSIFICATION"."BUSINESS_FEATURES_RAW"
WHERE valid_rec = TRUE AND processed = TRUE; 