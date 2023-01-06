-------------------------------------------------------------------
----------------- Stage for all ams_productlicenser tables
-------------------------------------------------------------------
create OR replace stage ZENPROD.AMS_PRODUCTLICENSER.AMS_PRODUCTLICENSER_S3_STAGE
    file_format = ( format_name = 'ZENPROD.AMS_PRODUCTLICENSER.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zp-uw2-data-archives/rds/ams-productlicenser/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-ams_productlicenser-snowflake-stage');

-- Create Schema in snowflake
create schema ZENPROD.AMS_PRODUCTLICENSER
