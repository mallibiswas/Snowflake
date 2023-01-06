-------------------------------------------------------------------
----------------- ARCHIVER_LOCAL_CLASSIFIER_HOURS_OVERRIDE_S3_STAGE Snowpipe
-------------------------------------------------------------------

create stage if not exists ZENSTAG.PRESENCE.ARCHIVER_LOCAL_CLASSIFIER_HOURS_OVERRIDE_S3_STAGE
    file_format = ZENSTAG.PRESENCE.PRESENCE_CSV_FORMAT
    url = 's3://zs-uw2-data-archives/rds/wifisightingsclassifierconfig/local_classifier_hours_override'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zs-uw2-rds-local-classifier-hours-override-snowflake-stage');

create or replace TABLE ZENSTAG.PRESENCE.LOCAL_CLASSIFIER_HOURS_OVERRIDE (
	ID VARCHAR(16777216),
	CONFIG_ID VARCHAR(16777216),
	OVERRIDE_DATE DATE,
	CLOSED BOOLEAN,
	BEGIN_TIME_MINUTES NUMBER(38,0),
	END_TIME_MINUTES NUMBER(38,0),
	ASOF_DATE DATE
);

create or replace TABLE ZENSTAG.PRESENCE.LOCAL_CLASSIFIER_HOURS_OVERRIDE_ARCHIVE (
-- Differently of JSON, values coming from CSV are spread out through N columns. We need to save them separately.
	ID VARCHAR(16777216),
	CONFIG_ID VARCHAR(16777216),
	OVERRIDE_DATE DATE,
	CLOSED BOOLEAN,
	BEGIN_TIME_MINUTES NUMBER(38,0),
	END_TIME_MINUTES NUMBER(38,0),
	INSERT_ID number AUTOINCREMENT
);

create pipe if not exists ZENSTAG.PRESENCE.LOCAL_CLASSIFIER_HOURS_OVERRIDE_SNOWPIPE auto_ingest=true as
COPY INTO ZENSTAG.PRESENCE.LOCAL_CLASSIFIER_HOURS_OVERRIDE_ARCHIVE (
    ID,
	CONFIG_ID,
	OVERRIDE_DATE,
	CLOSED,
	BEGIN_TIME_MINUTES,
	END_TIME_MINUTES
)
FROM @ZENSTAG.PRESENCE.ARCHIVER_LOCAL_CLASSIFIER_HOURS_OVERRIDE_S3_STAGE
on_error = 'continue';

create stream if not exists ZENSTAG.PRESENCE.LOCAL_CLASSIFIER_HOURS_OVERRIDE_CHANGES on table ZENSTAG.PRESENCE.LOCAL_CLASSIFIER_HOURS_OVERRIDE_ARCHIVE;

create task if not exists ZENSTAG.PRESENCE.LOCAL_CLASSIFIER_HOURS_OVERRIDE_UPSERT_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON * * * * * UTC'
WHEN
    SYSTEM$STREAM_HAS_DATA('ZENSTAG.PRESENCE.LOCAL_CLASSIFIER_HOURS_OVERRIDE_CHANGES')
AS
    MERGE INTO ZENSTAG.PRESENCE.LOCAL_CLASSIFIER_HOURS_OVERRIDE as lcho USING (
        SELECT
            id,
            config_id,
            override_date,
            closed,
            begin_time_minutes,
            end_time_minutes,
            current_date as asof_date
        FROM ZENSTAG.PRESENCE.LOCAL_CLASSIFIER_HOURS_OVERRIDE_CHANGES
    ) as lcho_changes on lcho_changes.id = lcho.id
    WHEN MATCHED THEN
        UPDATE SET
            lcho.config_id = lcho_changes.config_id,
            lcho.override_date = lcho_changes.override_date,
            lcho.closed = lcho_changes.closed,
            lcho.begin_time_minutes = lcho_changes.begin_time_minutes,
            lcho.end_time_minutes = lcho_changes.end_time_minutes,
            lcho.asof_date = lcho_changes.asof_date
    WHEN NOT MATCHED THEN
        INSERT VALUES (
            lcho_changes.id,
            lcho_changes.config_id,
            lcho_changes.override_date,
            lcho_changes.closed,
            lcho_changes.begin_time_minutes,
            lcho_changes.end_time_minutes,
            lcho_changes.asof_date
        );

ALTER TASK ZENSTAG.PRESENCE.LOCAL_CLASSIFIER_HOURS_OVERRIDE_UPSERT_TASK RESUME;
