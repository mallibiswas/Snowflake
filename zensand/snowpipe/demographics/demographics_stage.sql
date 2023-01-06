-------------------------------------------------------------------
----------------- Stage for all demographics tables
-------------------------------------------------------------------
create OR replace stage ZENSAND.DEMOGRAPHICS.DEMOGRAPHICS_S3_STAGE
    file_format = ( format_name = 'ZENSAND.DEMOGRAPHICS.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zd-uw2-data-archives/rds/demographics/'
    credentials = ( aws_role = 'arn:aws:iam::504740723475:role/zd-uw2-demographics-snowflake-stage');

-- Create Schema in snowflake
create schema ZENSAND.DEMOGRAPHICS;
