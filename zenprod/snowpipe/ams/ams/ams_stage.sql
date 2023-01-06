-------------------------------------------------------------------
----------------- Stage for all ams tables
-------------------------------------------------------------------
create OR replace stage ZENPROD.AMS.AMS_ACCOUNTS_S3_STAGE
    file_format = ( format_name = 'ZENPROD.AMS.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zp-uw2-data-archives/rds/ams/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-ams-snowflake-stage');

-- Create Schema in snowflake
create schema ZENPROD.AMS;