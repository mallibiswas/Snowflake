-------------------------------------------------------------------
----------------- Stage for all ams_acccount tables
-------------------------------------------------------------------
create OR replace stage ZENSAND.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE
    file_format = ( format_name = 'ZENSAND.AMS_ACCOUNTS.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zd-uw2-data-archives/rds/ams-accounts/'
    credentials = ( aws_role = 'arn:aws:iam::504740723475:role/zd-uw2-ams_accounts-snowflake-stage');

-- Create Schema in snowflake
create schema ZENSAND.AMS_ACCOUNTS;