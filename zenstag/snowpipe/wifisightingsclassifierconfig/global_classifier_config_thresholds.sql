-------------------------------------------------------------------
----------------- GLOBAL_CLASSIFIER_CONFIG_THRESHOLDS Snowpipe
-------------------------------------------------------------------

create stage if not exists ZENSTAG.PRESENCE.ARCHIVER_GLOBAL_CLASSIFIER_CONFIG_THRESHOLDS_S3_STAGE
    file_format = ZENSTAG.PRESENCE.PRESENCE_CSV_FORMAT
    url = 's3://zs-uw2-data-archives/rds/wifisightingsclassifierconfig/global_classifier_config_thresholds'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zs-uw2-rds-global-classifier-config-thresholds-snowflake-stage');

create or replace TABLE ZENSTAG.PRESENCE.GLOBAL_CLASSIFIER_CONFIG_THRESHOLDS (
	LOCK VARCHAR(16777216),
	DEFAULT_MIN_WALKIN_DWELL_TIME_SECONDS NUMBER(38,0),
	DEFAULT_MAX_WALKIN_DWELL_TIME_SECONDS NUMBER(38,0),
	DEFAULT_MAX_HUMAN_DWELL_TIME_SECONDS NUMBER(38,0),
	MIN_WALKIN_DWELL_TIME_SECONDS_LOWER_LIMIT NUMBER(38,0),
	MIN_WALKIN_DWELL_TIME_SECONDS_UPPER_LIMIT NUMBER(38,0),
	MAX_WALKIN_DWELL_TIME_SECONDS_LOWER_LIMIT NUMBER(38,0),
	MAX_WALKIN_DWELL_TIME_SECONDS_UPPER_LIMIT NUMBER(38,0),
	MAX_HUMAN_DWELL_TIME_SECONDS_LOWER_LIMIT NUMBER(38,0),
	MAX_HUMAN_DWELL_TIME_SECONDS_UPPER_LIMIT NUMBER(38,0),
	DEFAULT_MIN_WALKIN_SIGNAL_STRENGTH NUMBER(38,0),
	MIN_WALKIN_SIGNAL_STRENGTH_LOWER_LIMIT NUMBER(38,0),
	MIN_WALKIN_SIGNAL_STRENGTH_UPPER_LIMIT NUMBER(38,0),
	CREATED_AT TIMESTAMP_NTZ(9),
	MODIFIED_AT TIMESTAMP_NTZ(9),
	OPEN_ALLOWANCE_MINUTES NUMBER(38,0),
	CLOSE_ALLOWANCE_MINUTES NUMBER(38,0),
	ASOF_DATE DATE
);

create table if not exists ZENSTAG.PRESENCE.GLOBAL_CLASSIFIER_CONFIG_THRESHOLDS_ARCHIVE (
-- Differently of JSON, values coming from CSV are spread out through N columns. We need to save them separately.
	LOCK VARCHAR(16777216),
	DEFAULT_MIN_WALKIN_DWELL_TIME_SECONDS NUMBER(38,0),
	DEFAULT_MAX_WALKIN_DWELL_TIME_SECONDS NUMBER(38,0),
	DEFAULT_MAX_HUMAN_DWELL_TIME_SECONDS NUMBER(38,0),
	MIN_WALKIN_DWELL_TIME_SECONDS_LOWER_LIMIT NUMBER(38,0),
	MIN_WALKIN_DWELL_TIME_SECONDS_UPPER_LIMIT NUMBER(38,0),
	MAX_WALKIN_DWELL_TIME_SECONDS_LOWER_LIMIT NUMBER(38,0),
	MAX_WALKIN_DWELL_TIME_SECONDS_UPPER_LIMIT NUMBER(38,0),
	MAX_HUMAN_DWELL_TIME_SECONDS_LOWER_LIMIT NUMBER(38,0),
	MAX_HUMAN_DWELL_TIME_SECONDS_UPPER_LIMIT NUMBER(38,0),
	DEFAULT_MIN_WALKIN_SIGNAL_STRENGTH NUMBER(38,0),
	MIN_WALKIN_SIGNAL_STRENGTH_LOWER_LIMIT NUMBER(38,0),
	MIN_WALKIN_SIGNAL_STRENGTH_UPPER_LIMIT NUMBER(38,0),
	CREATED_AT TIMESTAMP_NTZ(9),
	MODIFIED_AT TIMESTAMP_NTZ(9),
	OPEN_ALLOWANCE_MINUTES NUMBER(38,0),
	CLOSE_ALLOWANCE_MINUTES NUMBER(38,0),
	INSERT_ID number AUTOINCREMENT
);

create pipe if not exists ZENSTAG.PRESENCE.GLOBAL_CLASSIFIER_CONFIG_THRESHOLDS_SNOWPIPE auto_ingest=true as
COPY INTO ZENSTAG.PRESENCE.GLOBAL_CLASSIFIER_CONFIG_THRESHOLDS_ARCHIVE (
	LOCK,
	DEFAULT_MIN_WALKIN_DWELL_TIME_SECONDS,
	DEFAULT_MAX_WALKIN_DWELL_TIME_SECONDS,
	DEFAULT_MAX_HUMAN_DWELL_TIME_SECONDS,
	MIN_WALKIN_DWELL_TIME_SECONDS_LOWER_LIMIT,
	MIN_WALKIN_DWELL_TIME_SECONDS_UPPER_LIMIT,
	MAX_WALKIN_DWELL_TIME_SECONDS_LOWER_LIMIT,
	MAX_WALKIN_DWELL_TIME_SECONDS_UPPER_LIMIT,
	MAX_HUMAN_DWELL_TIME_SECONDS_LOWER_LIMIT,
	MAX_HUMAN_DWELL_TIME_SECONDS_UPPER_LIMIT,
	DEFAULT_MIN_WALKIN_SIGNAL_STRENGTH,
	MIN_WALKIN_SIGNAL_STRENGTH_LOWER_LIMIT,
	MIN_WALKIN_SIGNAL_STRENGTH_UPPER_LIMIT,
	CREATED_AT,
	MODIFIED_AT,
	OPEN_ALLOWANCE_MINUTES,
	CLOSE_ALLOWANCE_MINUTES
)
FROM @ZENSTAG.PRESENCE.ARCHIVER_GLOBAL_CLASSIFIER_CONFIG_THRESHOLDS_S3_STAGE
on_error = 'continue';

create stream if not exists ZENSTAG.PRESENCE.GLOBAL_CLASSIFIER_CONFIG_THRESHOLDS_CHANGES on table ZENSTAG.PRESENCE.GLOBAL_CLASSIFIER_CONFIG_THRESHOLDS_ARCHIVE;

create task if not exists ZENSTAG.PRESENCE.GLOBAL_CLASSIFIER_CONFIG_THRESHOLDS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON * * * * * UTC'
WHEN
    SYSTEM$STREAM_HAS_DATA('ZENSTAG.PRESENCE.GLOBAL_CLASSIFIER_CONFIG_THRESHOLDS_CHANGES')
AS
    UPDATE ZENSTAG.PRESENCE.GLOBAL_CLASSIFIER_CONFIG_THRESHOLDS
        SET
            lock = global_changes.$1::string,
            default_min_walkin_dwell_time_seconds = global_changes.$2::integer,
            default_max_walkin_dwell_time_seconds = global_changes.$3::integer,
            default_max_human_dwell_time_seconds = global_changes.$4::integer,
            min_walkin_dwell_time_seconds_lower_limit = global_changes.$5::integer,
            min_walkin_dwell_time_seconds_upper_limit = global_changes.$6::integer,
            max_walkin_dwell_time_seconds_lower_limit = global_changes.$7::integer,
            max_walkin_dwell_time_seconds_upper_limit = global_changes.$8::integer,
            max_human_dwell_time_seconds_lower_limit = global_changes.$9::integer,
            max_human_dwell_time_seconds_upper_limit = global_changes.$10::integer,
            default_min_walkin_signal_strength = global_changes.$11::integer,
            min_walkin_signal_strength_lower_limit = global_changes.$12::integer,
            min_walkin_signal_strength_upper_limit = global_changes.$13::integer,
            created_at = global_changes.$14::timestamp,
            modified_at = global_changes.$15::timestamp,
            open_allowance_minutes = global_changes.$16::integer,
            close_allowance_minutes = global_changes.$17::integer,
            asof_date = current_date
        FROM ZENSTAG.PRESENCE.GLOBAL_CLASSIFIER_CONFIG_THRESHOLDS_CHANGES as global_changes
    ;

ALTER TASK ZENSTAG.PRESENCE.GLOBAL_CLASSIFIER_CONFIG_THRESHOLDS_TASK RESUME;
