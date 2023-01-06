-------------------------------------------------------------------
----------------- ARCHIVER_LOCATION_CLASSIFIER_CONFIG_THRESHOLDS_S3_STAGE Snowpipe
-------------------------------------------------------------------

create stage if not exists ZENSAND.PRESENCE.ARCHIVER_LOCATION_CLASSIFIER_CONFIG_THRESHOLDS_S3_STAGE
    file_format = ZENSAND.PRESENCE.PRESENCE_CSV_FORMAT
    url = 's3://zd-uw2-data-archives/rds/wifisightingsclassifierconfig/location_classifier_config_thresholds'
    credentials = ( aws_role = 'arn:aws:iam::504740723475:role/zd-uw2-rds-location-classifier-config-thresholds-snowflake-stage');

create or replace TABLE ZENSAND.PRESENCE.LOCATION_CLASSIFIER_CONFIG_THRESHOLDS (
	LOCATION_ID VARCHAR(16777216),
	MIN_WALKIN_DWELL_TIME_SECONDS NUMBER(38,0),
	MAX_WALKIN_DWELL_TIME_SECONDS NUMBER(38,0),
	MAX_HUMAN_DWELL_TIME_SECONDS NUMBER(38,0),
	MIN_WALKIN_SIGNAL_STRENGTH NUMBER(38,0),
	CREATED_AT TIMESTAMP_NTZ(9),
	MODIFIED_AT TIMESTAMP_NTZ(9),
	ID VARCHAR(16777216),
	EFFECTIVE_AS_OF TIMESTAMP_NTZ(9),
	TIME_ZONE VARCHAR(16777216),
	IS_AUTO_MIN_WALKIN_SIGNAL_STRENGTH BOOLEAN,
	ASOF_DATE DATE
);

create or replace TABLE ZENSAND.PRESENCE.LOCATION_CLASSIFIER_CONFIG_THRESHOLDS_ARCHIVE (
-- Differently of JSON, values coming from CSV are spread out through N columns. We need to save them separately.
	LOCATION_ID VARCHAR(16777216),
	MIN_WALKIN_DWELL_TIME_SECONDS NUMBER(38,0),
	MAX_WALKIN_DWELL_TIME_SECONDS NUMBER(38,0),
	MAX_HUMAN_DWELL_TIME_SECONDS NUMBER(38,0),
	MIN_WALKIN_SIGNAL_STRENGTH NUMBER(38,0),
	CREATED_AT TIMESTAMP_NTZ(9),
	MODIFIED_AT TIMESTAMP_NTZ(9),
	ID VARCHAR(16777216),
	EFFECTIVE_AS_OF TIMESTAMP_NTZ(9),
	TIME_ZONE VARCHAR(16777216),
	IS_AUTO_MIN_WALKIN_SIGNAL_STRENGTH BOOLEAN,
	INSERT_ID number AUTOINCREMENT
);

create pipe if not exists ZENSAND.PRESENCE.LOCATION_CLASSIFIER_CONFIG_THRESHOLDS_SNOWPIPE auto_ingest=true as
COPY INTO ZENSAND.PRESENCE.LOCATION_CLASSIFIER_CONFIG_THRESHOLDS_ARCHIVE (
	LOCATION_ID,
	MIN_WALKIN_DWELL_TIME_SECONDS,
	MAX_WALKIN_DWELL_TIME_SECONDS,
	MAX_HUMAN_DWELL_TIME_SECONDS,
	MIN_WALKIN_SIGNAL_STRENGTH,
	CREATED_AT,
	MODIFIED_AT,
	ID,
	EFFECTIVE_AS_OF,
	TIME_ZONE,
	IS_AUTO_MIN_WALKIN_SIGNAL_STRENGTH
)
FROM @ZENSAND.PRESENCE.ARCHIVER_LOCATION_CLASSIFIER_CONFIG_THRESHOLDS_S3_STAGE
on_error = 'continue';

create stream if not exists ZENSAND.PRESENCE.LOCATION_CLASSIFIER_CONFIG_THRESHOLDS_CHANGES on table ZENSAND.PRESENCE.LOCATION_CLASSIFIER_CONFIG_THRESHOLDS_ARCHIVE;

create task if not exists ZENSAND.PRESENCE.LOCATION_CLASSIFIER_CONFIG_THRESHOLDS_UPSERT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON * * * * * UTC'
WHEN
    SYSTEM$STREAM_HAS_DATA('ZENSAND.PRESENCE.LOCATION_CLASSIFIER_CONFIG_THRESHOLDS_CHANGES')
AS
    MERGE INTO ZENSAND.PRESENCE.LOCATION_CLASSIFIER_CONFIG_THRESHOLDS as lcgt USING (
        SELECT
            location_id,
            min_walkin_dwell_time_seconds,
            max_walkin_dwell_time_seconds,
            max_human_dwell_time_seconds,
            min_walkin_signal_strength,
            created_at,
            modified_at,
            id,
            effective_as_of,
            time_zone,
            is_auto_min_walkin_signal_strength,
            current_date as asof_date
        FROM ZENSAND.PRESENCE.LOCATION_CLASSIFIER_CONFIG_THRESHOLDS_CHANGES
    ) as lcgt_changes on lcgt_changes.id = lcgt.id
    WHEN MATCHED THEN
        UPDATE SET
            lcgt.location_id = lcgt_changes.location_id,
            lcgt.min_walkin_dwell_time_seconds = lcgt_changes.min_walkin_dwell_time_seconds,
            lcgt.max_walkin_dwell_time_seconds = lcgt_changes.max_walkin_dwell_time_seconds,
            lcgt.max_human_dwell_time_seconds = lcgt_changes.max_human_dwell_time_seconds,
            lcgt.min_walkin_signal_strength = lcgt_changes.min_walkin_signal_strength,
            lcgt.created_at = lcgt_changes.created_at,
            lcgt.modified_at = lcgt_changes.modified_at,
            lcgt.effective_as_of = lcgt_changes.effective_as_of,
            lcgt.time_zone = lcgt_changes.time_zone,
            lcgt.is_auto_min_walkin_signal_strength = lcgt_changes.is_auto_min_walkin_signal_strength,
            lcgt.asof_date = lcgt_changes.asof_date
    WHEN NOT MATCHED THEN
        INSERT VALUES (
            lcgt_changes.location_id,
            lcgt_changes.min_walkin_dwell_time_seconds,
            lcgt_changes.max_walkin_dwell_time_seconds,
            lcgt_changes.max_human_dwell_time_seconds,
            lcgt_changes.min_walkin_signal_strength,
            lcgt_changes.created_at,
            lcgt_changes.modified_at,
            lcgt_changes.id,
            lcgt_changes.effective_as_of,
            lcgt_changes.time_zone,
            lcgt_changes.is_auto_min_walkin_signal_strength,
            lcgt_changes.asof_date
        );

ALTER TASK ZENSAND.PRESENCE.LOCATION_CLASSIFIER_CONFIG_THRESHOLDS_UPSERT_TASK RESUME;
