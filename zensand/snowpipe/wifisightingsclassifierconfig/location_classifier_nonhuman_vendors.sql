-------------------------------------------------------------------
----------------- ARCHIVER_LOCATION_CLASSIFIER_NONHUMAN_VENDORS_S3_STAGE Snowpipe
-------------------------------------------------------------------

create stage if not exists ZENSAND.PRESENCE.ARCHIVER_LOCATION_CLASSIFIER_NONHUMAN_VENDORS_S3_STAGE
    file_format = ZENSAND.PRESENCE.PRESENCE_CSV_FORMAT
    url = 's3://zd-uw2-data-archives/rds/wifisightingsclassifierconfig/location_classifier_nonhuman_vendors'
    credentials = ( aws_role = 'arn:aws:iam::504740723475:role/zd-uw2-rds-location-classifier-nonhuman-vendors-snowflake-stage');

create or replace TABLE ZENSAND.PRESENCE.LOCATION_CLASSIFIER_NONHUMAN_VENDORS (
    LOCATION_ID VARCHAR(16777216),
    MAC_PREFIX VARCHAR(16777216),
    CREATED_AT DATE
);

create or replace TABLE ZENSAND.PRESENCE.LOCATION_CLASSIFIER_NONHUMAN_VENDORS_ARCHIVE (
-- Differently of JSON, values coming from CSV are spread out through N columns. We need to save them separately.
	LOCATION_ID VARCHAR(16777216),
    MAC_PREFIX VARCHAR(16777216),
    CREATED_AT DATE
);

create pipe if not exists ZENSAND.PRESENCE.LOCATION_CLASSIFIER_NONHUMAN_VENDORS_SNOWPIPE auto_ingest=true as
COPY INTO ZENSAND.PRESENCE.LOCATION_CLASSIFIER_NONHUMAN_VENDORS_ARCHIVE (
    LOCATION_ID,
    MAC_PREFIX,
    CREATED_AT
)
FROM @ZENSAND.PRESENCE.ARCHIVER_LOCATION_CLASSIFIER_NONHUMAN_VENDORS_S3_STAGE
on_error = 'continue';

create stream if not exists ZENSAND.PRESENCE.LOCATION_CLASSIFIER_NONHUMAN_VENDORS_CHANGES on table ZENSAND.PRESENCE.LOCATION_CLASSIFIER_NONHUMAN_VENDORS_ARCHIVE;


create task if not exists ZENSAND.PRESENCE.LOCATION_CLASSIFIER_NONHUMAN_VENDORS_UPSERT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON * * * * * UTC'
WHEN
    SYSTEM$STREAM_HAS_DATA('ZENSAND.PRESENCE.LOCATION_CLASSIFIER_NONHUMAN_VENDORS_CHANGES')
AS
    MERGE INTO ZENSAND.PRESENCE.LOCATION_CLASSIFIER_NONHUMAN_VENDORS as lcg USING (
        SELECT
            location_id,
            mac_prefix,
            created_at
        FROM ZENSAND.PRESENCE.LOCATION_CLASSIFIER_NONHUMAN_VENDORS_CHANGES
    ) as lcg_changes on lcg_changes.config_id = lcg.config_id
    WHEN MATCHED THEN
        UPDATE SET
            lcg.location_id = lcg_changes.location_id,
            lcg.mac_prefix = lcg_changes.mac_prefix,
            lcg.created_at = lcg_changes.created_at
    WHEN NOT MATCHED THEN
        INSERT VALUES (
            lcg_changes.location_id,
            lcg_changes.mac_prefix,
            lcg_changes.created_at
        );

ALTER TASK ZENSAND.PRESENCE.LOCATION_CLASSIFIER_NONHUMAN_VENDORS_UPSERT_TASK RESUME;
