CREATE OR REPLACE MATERIALIZED VIEW BUSINESS_FEATURES_MANUAL_REVIEW
comment='Records that failed auto-processing and requires manual review' AS
SELECT 
    bfr.BUSINESS_ID as business_id
    , bfr.PARENT_ID as parent_id
    , bfr.PARENT_NAME as Z_parent_name
    , bfr.BUSINESS_NAME as Z_business_name
    , bfr.GOOGLE:name::string as google_name
    , CONCAT(bfr.ADDRESS:street, ', ', bfr.ADDRESS:city, ', ', bfr.ADDRESS:state, ' ', bfr.ADDRESS:zipcode, ', ', bfr.ADDRESS:country) as Z_address
    , bfr.GOOGLE:formatted_address::string as google_address
    , bfr.GOOGLE:types as google_types
    , bfr.FOURSQUARE:primary_category::string as foursquare_primary_category
FROM ZENALYTICS.BUSINESS_FEATURES.BUSINESS_FEATURES_RAW bfr
WHERE valid_rec = True AND processed = True and manual_review = True;