-------------------------------------------------------------------
----------------- Stage for all crm tables
-------------------------------------------------------------------
create OR replace stage ZENSAND.CRM.MONGO_S3_STAGE
    file_format = ( format_name = 'ZENSAND.CRM.S3_MONGO_FORMAT')
    url = 's3://zd-uw2-data-archives/mongo/'
    credentials = ( aws_role = 'arn:aws:iam::504740723475:role/zd-uw2-mongo2-snowflake-stage');
