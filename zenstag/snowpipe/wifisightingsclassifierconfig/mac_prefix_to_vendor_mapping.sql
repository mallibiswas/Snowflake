-------------------------------------------------------------------
----------------- ARCHIVER_MAC_PREFIX_TO_VENDOR_MAPPING_S3_STAGE Snowpipe
-------------------------------------------------------------------

create stage if not exists ZENSTAG.PRESENCE.ARCHIVER_MAC_PREFIX_TO_VENDOR_MAPPING_S3_STAGE
    file_format = ZENSTAG.PRESENCE.PRESENCE_CSV_FORMAT
    url = 's3://zs-uw2-data-archives/rds/wifisightingsclassifierconfig/mac_prefix_to_vendor_mapping'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zs-uw2-rds-mac-prefix-to-vendor-mapping-snowflake-stage');

create or replace TABLE ZENSTAG.PRESENCE.MAC_PREFIX_TO_VENDOR_MAPPING (
	MAC_PREFIX VARCHAR(16777216),
	VENDOR_NAME VARCHAR(16777216),
	NOT_HUMAN BOOLEAN,
	CREATED_AT TIMESTAMP_NTZ(9),
	MODIFIED_AT TIMESTAMP_NTZ(9),
	ASOF_DATE DATE
);

create or replace TABLE ZENSTAG.PRESENCE.MAC_PREFIX_TO_VENDOR_MAPPING_ARCHIVE (
-- Differently of JSON, values coming from CSV are spread out through N columns. We need to save them separately.
	MAC_PREFIX VARCHAR(16777216),
	VENDOR_NAME VARCHAR(16777216),
	NOT_HUMAN BOOLEAN,
	CREATED_AT TIMESTAMP_NTZ(9),
	MODIFIED_AT TIMESTAMP_NTZ(9),
	INSERT_ID number AUTOINCREMENT
);

create pipe if not exists ZENSTAG.PRESENCE.MAC_PREFIX_TO_VENDOR_MAPPING_SNOWPIPE auto_ingest=true as
COPY INTO ZENSTAG.PRESENCE.MAC_PREFIX_TO_VENDOR_MAPPING_ARCHIVE (
    MAC_PREFIX,
    VENDOR_NAME,
    NOT_HUMAN,
    CREATED_AT,
    MODIFIED_AT
)
FROM @ZENSTAG.PRESENCE.ARCHIVER_MAC_PREFIX_TO_VENDOR_MAPPING_S3_STAGE
on_error = 'continue';

create stream if not exists ZENSTAG.PRESENCE.MAC_PREFIX_TO_VENDOR_MAPPING_CHANGES on table ZENSTAG.PRESENCE.MAC_PREFIX_TO_VENDOR_MAPPING_ARCHIVE;

create task if not exists ZENSTAG.PRESENCE.MAC_PREFIX_TO_VENDOR_MAPPING_UPSERT_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON * * * * * UTC'
WHEN
    SYSTEM$STREAM_HAS_DATA('ZENSTAG.PRESENCE.MAC_PREFIX_TO_VENDOR_MAPPING_CHANGES')
AS
    MERGE INTO ZENSTAG.PRESENCE.MAC_PREFIX_TO_VENDOR_MAPPING as mp USING (
        SELECT
            mac_prefix,
            vendor_name,
            not_human,
            created_at,
            modified_at,
            current_date as asof_date
        FROM ZENSTAG.PRESENCE.MAC_PREFIX_TO_VENDOR_MAPPING_CHANGES
    ) as mp_changes on mp.mac_prefix = mp_changes.mac_prefix
    WHEN MATCHED THEN
        UPDATE SET
            mp.vendor_name = mp_changes.vendor_name,
            mp.not_human = mp_changes.not_human,
            mp.created_at = mp_changes.created_at,
            mp.modified_at = mp_changes.modified_at,
            mp.asof_date = mp_changes.asof_date
    WHEN NOT MATCHED THEN
        INSERT VALUES (
            mp_changes.mac_prefix,
            mp_changes.vendor_name,
            mp_changes.not_human,
            mp_changes.created_at,
            mp_changes.modified_at,
            mp_changes.asof_date
        );

ALTER TASK ZENSTAG.PRESENCE.MAC_PREFIX_TO_VENDOR_MAPPING_UPSERT_TASK RESUME;
