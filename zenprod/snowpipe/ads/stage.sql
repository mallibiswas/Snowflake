-- Create stage for all ads s3 access
Create stage if not exists ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE
    file_format = ZENPROD.ADS.RDS_EXPORTER_CSV_FORMAT
    url = 's3://zp-uw2-data-archives/rds'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-rds-ads-snowflake-stage');
