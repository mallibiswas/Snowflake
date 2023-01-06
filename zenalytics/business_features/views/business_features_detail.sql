-- BUSINESS_FEATURES_DETAIL VIEW
CREATE OR REPLACE VIEW ZENDEV.BUSINESS_CLASSIFICATION.BUSINESS_FEATURES_DETAIL
COMMENT ='business details collected from 3rd party sources' AS
select 
  business_id AS business_id
  , permanently_closed AS permanently_closed
  , google:name::string as google_name
  , google:number_of_reviews as google_number_of_reviews
  , google:price as google_price
  , google:rating as google_rating
  , foursquare:hours as foursquare_hours
from "ZENDEV"."BUSINESS_CLASSIFICATION"."BUSINESS_FEATURES_RAW"
WHERE valid_rec = TRUE AND processed = TRUE; 
