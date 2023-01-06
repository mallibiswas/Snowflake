-------------------------------------------------------------------
----------------- Stage for all hotspot tables
-------------------------------------------------------------------
create OR replace stage ZENSTAG.HOTSPOT2.HOTSPOT2_S3_STAGE
    file_format = ( format_name = 'ZENSTAG.HOTSPOT2.S3_RDS_EXPORTER_CSV_FORMAT')
    url = 's3://zs-uw2-data-archives/rds/hotspot2/'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zs-uw2-hotspot2-snowflake-stage');

-- Create Schema in snowflake
create schema ZENSTAG.HOTSPOT2;
