-------------------------------------------------------------------
----------------- Stage for all ams_acccount tables
-------------------------------------------------------------------
create OR replace stage ZENSTAG.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE
    file_format = ( format_name = 'ZENSTAG.AMS_ACCOUNTS.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zs-uw2-data-archives/rds/ams-accounts/'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zs-uw2-ams_accounts-snowflake-stage');

-- Create Schema in snowflake
create schema ZENSTAG.AMS_ACCOUNTS;