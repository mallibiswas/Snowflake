-------------------------------------------------------------------
----------------- Stage for all ams_productlicenser tables
-------------------------------------------------------------------
create OR replace stage ZENSAND.AMS_PRODUCTLICENSER.AMS_PRODUCTLICENSER_S3_STAGE
    file_format = ( format_name = 'ZENSAND.AMS_PRODUCTLICENSER.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zd-uw2-data-archives/rds/ams-productlicenser/'
    credentials = ( aws_role = 'arn:aws:iam::504740723475:role/zd-uw2-ams_productlicenser-snowflake-stage');

-- Create Schema in snowflake
create schema ZENSAND.AMS_PRODUCTLICENSER
