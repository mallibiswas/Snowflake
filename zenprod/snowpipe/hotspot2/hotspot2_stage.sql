-------------------------------------------------------------------
----------------- Stage for all hotspot tables
-------------------------------------------------------------------
create OR replace stage ZENPROD.HOTSPOT2.HOTSPOT2_S3_STAGE
    file_format = ( format_name = 'ZENPROD.HOTSPOT2.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zp-uw2-data-archives/rds/hotspot2/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-hotspot2-snowflake-stage');

-- Create Schema in snowflake
create schema ZENPROD.HOTSPOT2;
