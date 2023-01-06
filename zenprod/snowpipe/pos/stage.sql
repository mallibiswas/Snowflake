-- Create stage for all pos s3 access
Create stage if not exists ZENPROD.POS.ARCHIVER_POS_S3_STAGE
    file_format = ZENPROD.POS.RDS_EXPORTER_CSV_FORMAT
    url = 's3://zp-uw2-data-archives/rds/pos'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-rds-pos-snowflake-stage');
