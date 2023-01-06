-------------------------------------------------------------------
----------------- Stage for all ams_routers tables
-------------------------------------------------------------------
create OR replace stage ZENPROD.AMS_ROUTERS.AMS_ROUTERS_S3_STAGE
    file_format = ( format_name = 'ZENPROD.AMS_ROUTERS.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zp-uw2-data-archives/rds/ams-routers/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-ams_routers-snowflake-stage');
    
    
create schema ZENPROD.AMS_ROUTERS;
