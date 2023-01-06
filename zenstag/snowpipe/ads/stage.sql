-- Create stage for all ads s3 access
Create stage if not exists ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE
    file_format = ZENSTAG.ADS.RDS_EXPORTER_CSV_FORMAT
    url = 's3://zs-uw2-data-archives/rds'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zs-uw2-rds-ads-snowflake-stage');
