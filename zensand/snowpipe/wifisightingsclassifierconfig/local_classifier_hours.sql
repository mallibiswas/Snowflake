-------------------------------------------------------------------
----------------- ARCHIVER_LOCAL_CLASSIFIER_HOURS_S3_STAGE Snowpipe
-------------------------------------------------------------------

create stage if not exists ZENSAND.PRESENCE.ARCHIVER_LOCAL_CLASSIFIER_HOURS_S3_STAGE
    file_format = ZENSAND.PRESENCE.PRESENCE_CSV_FORMAT
    url = 's3://zd-uw2-data-archives/rds/wifisightingsclassifierconfig/local_classifier_hours/'
    credentials = ( aws_role = 'arn:aws:iam::504740723475:role/zd-uw2-rds-local-classifier-hours-snowflake-stage');

create or replace TABLE ZENSAND.PRESENCE.LOCAL_CLASSIFIER_HOURS (
	CONFIG_ID VARCHAR(16777216),
	DAY_OF_WEEK NUMBER(38,0),
	OPEN_TIME_MINUTES NUMBER(38,0),
	CLOSE_TIME_MINUTES NUMBER(38,0),
	OPEN_ALLOWANCE_MINUTES NUMBER(38,0),
	CLOSE_ALLOWANCE_MINUTES NUMBER(38,0),
	ASOF_DATE DATE
);

create or replace TABLE ZENSAND.PRESENCE.LOCAL_CLASSIFIER_HOURS_ARCHIVE (
-- Differently of JSON, values coming from CSV are spread out through N columns. We need to save them separately.
	CONFIG_ID VARCHAR(16777216),
	DAY_OF_WEEK NUMBER(38,0),
	OPEN_TIME_MINUTES NUMBER(38,0),
	CLOSE_TIME_MINUTES NUMBER(38,0),
	OPEN_ALLOWANCE_MINUTES NUMBER(38,0),
	CLOSE_ALLOWANCE_MINUTES NUMBER(38,0),
	INSERT_ID number AUTOINCREMENT
);

create pipe if not exists ZENSAND.PRESENCE.LOCAL_CLASSIFIER_HOURS_SNOWPIPE auto_ingest=true as
COPY INTO ZENSAND.PRESENCE.LOCAL_CLASSIFIER_HOURS_ARCHIVE (
    CONFIG_ID,
    DAY_OF_WEEK,
    OPEN_TIME_MINUTES,
    CLOSE_TIME_MINUTES,
    OPEN_ALLOWANCE_MINUTES,
    CLOSE_ALLOWANCE_MINUTES
)
FROM @ZENSAND.PRESENCE.ARCHIVER_LOCAL_CLASSIFIER_HOURS_S3_STAGE
on_error = 'continue';

create stream if not exists ZENSAND.PRESENCE.LOCAL_CLASSIFIER_HOURS_CHANGES on table ZENSAND.PRESENCE.LOCAL_CLASSIFIER_HOURS_ARCHIVE;


create task if not exists ZENSAND.PRESENCE.LOCAL_CLASSIFIER_HOURS_UPSERT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON * * * * * UTC'
WHEN
    SYSTEM$STREAM_HAS_DATA('ZENSAND.PRESENCE.LOCAL_CLASSIFIER_HOURS_CHANGES')
AS
    MERGE INTO ZENSAND.PRESENCE.LOCAL_CLASSIFIER_HOURS as lcg USING (
        SELECT
            config_id,
            day_of_week,
            open_time_minutes,
            close_time_minutes,
            open_allowance_minutes,
            close_allowance_minutes,
            current_date as asof_date
        FROM ZENSAND.PRESENCE.LOCAL_CLASSIFIER_HOURS_CHANGES
    ) as lcg_changes on lcg_changes.config_id = lcg.config_id and lcg_changes.day_of_week=lcg.day_of_week
    WHEN MATCHED THEN
        UPDATE SET
            lcg.open_time_minutes = lcg_changes.open_time_minutes,
            lcg.close_time_minutes = lcg_changes.close_time_minutes,
            lcg.open_allowance_minutes = lcg_changes.open_allowance_minutes,
            lcg.close_allowance_minutes = lcg_changes.close_allowance_minutes,
            lcg.asof_date = lcg_changes.asof_date
    WHEN NOT MATCHED THEN
        INSERT VALUES (
            lcg_changes.config_id,
            lcg_changes.day_of_week,
            lcg_changes.open_time_minutes,
            lcg_changes.close_time_minutes,
            lcg_changes.open_allowance_minutes,
            lcg_changes.close_allowance_minutes,
            lcg_changes.asof_date
        );

ALTER TASK ZENSAND.PRESENCE.LOCAL_CLASSIFIER_HOURS_UPSERT_TASK RESUME;
