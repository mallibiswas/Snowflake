-------------------------------------------------------------------
----------------- Stage for all crm tables
-------------------------------------------------------------------
create OR replace stage ZENPROD.CRM.MONGO_S3_STAGE
    file_format = ( format_name = 'ZENPROD.CRM.S3_MONGO_FORMAT')
    url = 's3://zp-uw2-data-archives/mongo/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-mongo2-snowflake-stage');
