create or replace table zenalytics.business_features.foursquare
(
BUSINESS_ID VARCHAR(16777216),
FOURSQUARE_ID VARCHAR(16777216),
FOURSQUARE_DUMP VARIANT,
INSERT_DTTM TIMESTAMP_NTZ(9)
)