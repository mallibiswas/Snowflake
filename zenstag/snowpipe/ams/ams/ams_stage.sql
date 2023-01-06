-------------------------------------------------------------------
----------------- Stage for all ams tables
-------------------------------------------------------------------
create OR replace stage ZENSTAG.AMS.AMS_ACCOUNTS_S3_STAGE
    file_format = ( format_name = 'ZENSTAG.AMS.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zs-uw2-data-archives/rds/ams/'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zs-uw2-ams-snowflake-stage');

-- Create Schema in snowflake
create schema ZENSTAG.AMS;