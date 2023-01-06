-- Create stage for all pos s3 access
Create stage if not exists ZENSAND.POS.ARCHIVER_POS_S3_STAGE
    file_format = ZENSAND.POS.RDS_EXPORTER_CSV_FORMAT
    url = 's3://zd-uw2-data-archives/rds/pos'
    credentials = ( aws_role = 'arn:aws:iam::504740723475:role/zd-uw2-rds-pos-snowflake-stage');
