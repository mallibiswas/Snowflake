-------------------------------------------------------------------
----------------- Stage for all ams tables
-------------------------------------------------------------------
create OR replace stage ZENSAND.AMS.AMS_ACCOUNTS_S3_STAGE
    file_format = ( format_name = 'ZENSAND.AMS.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zd-uw2-data-archives/rds/ams/'
    credentials = ( aws_role = 'arn:aws:iam::504740723475:role/zd-uw2-ams-snowflake-stage');

-- Create Schema in snowflake
create schema ZENSAND.AMS_ACCOUNTS;