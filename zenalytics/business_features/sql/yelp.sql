create or replace table zenalytics.business_features.yelp
(
BUSINESS_ID VARCHAR(16777216),
YELP_ID VARCHAR(16777216),
YELP_DUMP VARIANT,
INSERT_DTTM TIMESTAMP_NTZ(9)
)
