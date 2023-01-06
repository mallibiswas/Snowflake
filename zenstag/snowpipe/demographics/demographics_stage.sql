-------------------------------------------------------------------
----------------- Stage for all demographics tables
-------------------------------------------------------------------
create OR replace stage ZENSTAG.DEMOGRAPHICS.DEMOGRAPHICS_S3_STAGE
    file_format = ( format_name = 'ZENSTAG.DEMOGRAPHICS.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zp-uw2-data-archives/rds/demographics/'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zp-uw2-demographics-snowflake-stage');

-- Create Schema in snowflake
create schema ZENSTAG.DEMOGRAPHICS;
