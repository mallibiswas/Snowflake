-------------------------------------------------------------------
----------------- Stage for all ams_routers tables
-------------------------------------------------------------------
create OR replace stage ZENSTAG.AMS_ROUTERS.AMS_ROUTERS_S3_STAGE
    file_format = ( format_name = 'ZENSTAG.AMS_ROUTERS.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zs-uw2-data-archives/rds/ams-routers/'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zs-uw2-ams_routers-snowflake-stage');

-- Create Schema in snowflake
create schema ZENSTAG.AMS_ROUTERS;