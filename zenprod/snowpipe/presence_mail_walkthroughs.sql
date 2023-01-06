-------------------------------------------------------------------
----------------- MAIL_WALKTHROUGHS Snowpipe
-------------------------------------------------------------------

CREATE OR REPLACE STAGE ZENPROD.PRESENCE.MAIL_WALKTHROUGHS_S3_STAGE
    file_format = ( TYPE = JSON )
    url = 's3://zp-uw2-foundation-kafka-archives/archiver/mail_email_walkthroughs_bymessagelogid/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-archiver-mail-walkthroughs-snowflake-stage');


CREATE OR REPLACE TABLE ZENPROD.PRESENCE.MAIL_WALKTHROUGHS(
    -- Message log for this walkthrough.
    MESSAGELOG_ID VARCHAR(16777216),
    -- The ID of the business that sent the email.
    BUSINESS_ID VARCHAR(16777216),
    -- The ID of the contact (aka userprofile).
    CONTACT_ID VARCHAR(16777216),
    -- The location that the sighting occurred at.
    VISITED_LOCATION_ID VARCHAR(16777216),
    -- Timestamp of when the email was delivered.
    DELIVERED_TS timestamp_ntz(9),
    -- The sighting ID.
    SIGHTING_ID VARCHAR(16777216),
    -- Start time of the sighting.
    SIGHTING_START_TIME TIMESTAMP_NTZ(9),
    -- End time of the sighting.
    SIGHTING_END_TIME TIMESTAMP_NTZ(9),

    ASOF_DATE date
);

CREATE OR REPLACE TABLE ZENPROD.PRESENCE.MAIL_WALKTHROUGHS_ARCHIVE(
    RAW_RECORD VARIANT, -- Store raw so pipe doesnt need to be torn down on schema update
    INSERT_ID number AUTOINCREMENT
);


-- Pipe will insert raw data from stage to our raw archive table
CREATE PIPE IF NOT EXISTS ZENPROD.PRESENCE.MAIL_WALKTHROUGHS_SNOWPIPE auto_ingest=true as
COPY INTO ZENPROD.PRESENCE.MAIL_WALKTHROUGHS_ARCHIVE(RAW_RECORD)
FROM @ZENPROD.PRESENCE.MAIL_WALKTHROUGHS_S3_STAGE
on_error = 'continue';


-- Stream handles data access so that DML's do not consume the same records twice. Think of it like an offset.
-- Our task will read off this steam periodically only receiving new records.
-- Note a DML statement is required for the offest to progress. A simple select statement will have no effect to offset advancement
CREATE STREAM IF NOT EXISTS ZENPROD.PRESENCE.MAIL_WALKTHROUGHS_ARCHIVE_CHANGES ON TABLE ZENPROD.PRESENCE.MAIL_WALKTHROUGHS_ARCHIVE;

-- Task will run like a typical cron job. This task will read off the stream, transform the raw data and run an upsert against sighting_id.
create task if not exists ZENPROD.PRESENCE.MAIL_WALKTHROUGHS_UPSERT_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON * * * * * UTC'
WHEN
    SYSTEM$STREAM_HAS_DATA('ZENPROD.PRESENCE.MAIL_WALKTHROUGHS_ARCHIVE_CHANGES')
AS
    MERGE INTO ZENPROD.PRESENCE.MAIL_WALKTHROUGHS as wt USING (
        SELECT
            $1:messagelog_id::string as messagelog_id,
            $1:business_id::string as business_id,
            $1:contact_id::string as contact_id,
            $1:visited_location_id::string as visited_location_id,
            TO_TIMESTAMP_NTZ($1:delivered_ts) as delivered_ts,
            $1:sighting_id::string as sighting_id,
            TO_TIMESTAMP_NTZ($1:sighting_start_time) as sighting_start_time,
            TO_TIMESTAMP_NTZ($1:sighting_end_time) as sighting_end_time,
            current_date as asof_date
        FROM ZENPROD.PRESENCE.MAIL_WALKTHROUGHS_ARCHIVE_CHANGES QUALIFY ROW_NUMBER() OVER (PARTITION BY messagelog_id order by INSERT_ID desc) = 1
    ) as wt_changes on wt_changes.messagelog_id = wt.messagelog_id
    WHEN MATCHED THEN
        UPDATE SET
            wt.business_id = wt_changes.business_id,
            wt.contact_id = wt_changes.contact_id,
            wt.visited_location_id = wt_changes.visited_location_id,
            wt.delivered_ts = wt_changes.delivered_ts,
            wt.sighting_id = wt_changes.sighting_id,
            wt.sighting_start_time = wt_changes.sighting_start_time,
            wt.sighting_end_time = wt_changes.sighting_end_time,
            wt.asof_date = wt_changes.asof_date
    WHEN NOT MATCHED THEN
        INSERT (
            messagelog_id,
            business_id,
            contact_id,
            visited_location_id,
            delivered_ts,
            sighting_id,
            sighting_start_time,
            sighting_end_time,
            asof_date
        ) VALUES (
            wt_changes.messagelog_id,
            wt_changes.business_id,
            wt_changes.contact_id,
            wt_changes.visited_location_id,
            wt_changes.delivered_ts,
            wt_changes.sighting_id,
            wt_changes.sighting_start_time,
            wt_changes.sighting_end_time,
            wt_changes.asof_date
        );

ALTER TASK ZENPROD.PRESENCE.MAIL_WALKTHROUGHS_UPSERT_TASK RESUME;
