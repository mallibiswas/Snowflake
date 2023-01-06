-------------------------------------------------------------------
----------------- Stage for all ams_productlicenser tables
-------------------------------------------------------------------
create OR replace stage ZENSTAG.AMS_PRODUCTLICENSER.AMS_PRODUCTLICENSER_S3_STAGE
    file_format = ( format_name = 'ZENSTAG.AMS_PRODUCTLICENSER.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zs-uw2-data-archives/rds/ams-productlicenser/'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zs-uw2-ams_productlicenser-snowflake-stage');

-- Create Schema in snowflake
create schema ZENSTAG.AMS_PRODUCTLICENSER
