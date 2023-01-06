-- Create stage for all pos s3 access
Create stage if not exists ZENSTAG.POS.ARCHIVER_POS_S3_STAGE
    file_format = ZENSTAG.POS.RDS_EXPORTER_CSV_FORMAT
    url = 's3://zs-uw2-data-archives/rds/pos'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zs-uw2-rds-pos-snowflake-stage');
