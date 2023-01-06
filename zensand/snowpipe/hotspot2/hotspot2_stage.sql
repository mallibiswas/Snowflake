-------------------------------------------------------------------
----------------- Stage for all hotspot tables
-------------------------------------------------------------------
create OR replace stage ZENSAND.HOTSPOT2.HOTSPOT2_S3_STAGE
    file_format = ( format_name = 'ZENSAND.HOTSPOT2.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zd-uw2-data-archives/rds/hotspot2/'
    credentials = ( aws_role = 'arn:aws:iam::504740723475:role/zd-uw2-hotspot2-snowflake-stage');

-- Create Schema in snowflake
create schema ZENSAND.HOTSPOT2;
