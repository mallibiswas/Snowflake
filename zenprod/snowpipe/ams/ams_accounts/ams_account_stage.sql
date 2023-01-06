-------------------------------------------------------------------
----------------- Stage for all ams_acccount tables
-------------------------------------------------------------------
create OR replace stage ZENPROD.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE
    file_format = ( format_name = 'ZENPROD.AMS_ACCOUNTS.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zp-uw2-data-archives/rds/ams-accounts/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-ams_accounts-snowflake-stage');

-- Create Schema in snowflake
create schema ZENPROD.AMS_ACCOUNTS;