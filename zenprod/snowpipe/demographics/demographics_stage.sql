-------------------------------------------------------------------
----------------- Stage for all demographics tables
-------------------------------------------------------------------
create OR replace stage ZENPROD.DEMOGRAPHICS.DEMOGRAPHICS_S3_STAGE
    file_format = ( format_name = 'ZENPROD.DEMOGRAPHICS.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zp-uw2-data-archives/rds/demographics/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-demographics-snowflake-stage');

-- Create Schema in snowflake
create schema ZENPROD.DEMOGRAPHICS;
