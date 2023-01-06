-------------------------------------------------------------------
----------------- Stage for all crm tables
-------------------------------------------------------------------
create OR replace stage ZENSTAG.CRM.MONGO_S3_STAGE
    file_format = ( format_name = 'ZENSTAG.CRM.S3_MONGO_FORMAT')
    url = 's3://zs-uw2-data-archives/mongo/'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zs-uw2-mongo2-snowflake-stage');
