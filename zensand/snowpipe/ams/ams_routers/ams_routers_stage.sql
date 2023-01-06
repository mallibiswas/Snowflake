-------------------------------------------------------------------
----------------- Stage for all ams_routers tables
-------------------------------------------------------------------
create OR replace stage ZENSAND.AMS_ROUTERS.AMS_ROUTERS_S3_STAGE
    file_format = ( format_name = 'ZENSAND.AMS_ROUTERS.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zd-uw2-data-archives/rds/ams-routers/'
    credentials = ( aws_role = 'arn:aws:iam::504740723475:role/zd-uw2-ams_routers-snowflake-stage');

-- Create Schema in snowflake
create schema ZENSAND.AMS_ROUTERS;
