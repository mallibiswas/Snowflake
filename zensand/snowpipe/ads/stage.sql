-- Create stage for all ads s3 access
Create stage if not exists ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE
    file_format = ZENSAND.ADS.RDS_EXPORTER_CSV_FORMAT
    url = 's3://zd-uw2-data-archives/rds'
    credentials = ( aws_role = 'arn:aws:iam::504740723475:role/zd-uw2-rds-ads-snowflake-stage');
