create stage if not exists ZENPROD.PRESENCE.ARCHIVER_WIFI_FINISHED_SIGHTINGS_S3_STAGE
    file_format = ( TYPE = JSON )
    url = 's3://zp-uw2-foundation-kafka-archives/archiver/presence_wifi_finishedsighting_bycontactidlocationid/'
    credentials = ( aws_role = 'arn:aws:iam::918769896082:role/zp-uw2-archiver-wifi-finishedsighting-snowflake-stage');

create table if not exists ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS (
    SIGHTING_ID VARCHAR(16777216),
    CLASSIFICATION VARCHAR(16777216),
    CLASSIFICATION_REASONS ARRAY,

    START_TIME TIMESTAMP,
    END_TIME TIMESTAMP,

    BLIP_COUNT NUMBER(38,0),
    MAX_RSSI INTEGER,
    MIN_RSSI INTEGER,
    AVG_RSSI FLOAT,
    PORTAL_BLIP_COUNT INTEGER,

    CLIENT_MAC_INFO ARRAY,

    CONTACT_ID VARCHAR(16777216),
    CONTACT_INFO VARCHAR(16777216),
    CONTACT_METHOD VARCHAR(16777216),

    LOCATION_ID VARCHAR(16777216),
    ACCOUNT_ID VARCHAR(16777216),

    KNOWN_TO_ZENREACH BOOLEAN,
    KNOWN_TO_MERCHANT_ACCOUNT BOOLEAN,
    KNOWN_TO_MERCHANT_LOCATION BOOLEAN,

    PRIVACY_VERSION VARCHAR(16777216),
    TERMS_VERSION VARCHAR(16777216),
    BUNDLE_VERSION VARCHAR(16777216),

    IS_EMPLOYEE BOOLEAN,

    PORTAL_BLIP_COUNT NUMBER(38,0)
);

-- Table will hold raw archiver data from our snowpipe
create table if not exists ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS_ARCHIVE (
    RAW_RECORD VARIANT, -- Store raw so pipe doesnt need to be torn down on schema update
    INSERT_ID NUMBER AUTOINCREMENT
);

-- Pipe will insert raw data from stage to our raw archive table
create pipe if not exists ZENPROD.PRESENCE.ARCHIVER_WIFI_FINISHED_SIGHTINGS_SNOWPIPE auto_ingest=true as
COPY INTO ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS_ARCHIVE(RAW_RECORD)
FROM @ZENPROD.PRESENCE.ARCHIVER_WIFI_FINISHED_SIGHTINGS_S3_STAGE
on_error = 'continue';

-- Steam handles data access so that DML's do not consume the same records twice. Think of it like an offset.
-- Our task will read off this steam periodically only receiving new records. 
-- Note a DML statement is required for the offest to progress. A simple select statment will have no effect to offset advancement
create stream if not exists ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS_ARCHIVE_CHANGES on table ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS_ARCHIVE;

-- Task will run like a typical cron job. This task will read off the stream, transform the raw data and run an upsert against sighting_id.
create or replace task ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS_UPSERT_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON * * * * * UTC'
WHEN
    SYSTEM$STREAM_HAS_DATA('ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS_ARCHIVE_CHANGES')
AS
    MERGE INTO ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS as wfs USING (
        SELECT
            HEX_ENCODE(BASE64_DECODE_BINARY($1:sighting_id::string)) as sighting_id,
            nvl($1:classification,'Classification_UNKNOWN')::string as classification,
            $1:classification_reasons::array as classification_reasons,

            TO_TIMESTAMP_NTZ($1:start_time) as start_time,
            TO_TIMESTAMP_NTZ($1:end_time) as end_time,

            nvl($1:stats:blip_count, 0)::integer as blip_count,
            nvl($1:stats:max_rssi, 0)::integer as max_rssi,
            nvl($1:stats:min_rssi, 0)::integer as min_rssi,
            nvl($1:stats:avg_rssi, 0)::float as avg_rssi,

            ZENPROD.PRESENCE.DECODE_CLIENT_MAC_INFO($1:client_mac_info::array) as client_mac_info,

            HEX_ENCODE(BASE64_DECODE_BINARY($1:contact_info:contact_id::string)) as contact_id,
            $1:contact_info:contact_info::string as contact_info,
            $1:contact_info:contact_method::string as contact_method,

            HEX_ENCODE(BASE64_DECODE_BINARY($1:location_info:location_id::string)) as location_id,
            HEX_ENCODE(BASE64_DECODE_BINARY($1:location_info:account_id::string)) as account_id,

            nvl($1:consent_info:known_to_zenreach, false)::boolean as known_to_zenreach,
            nvl($1:consent_info:known_to_merchant_account, false)::boolean as known_to_merchant_account,
            nvl($1:consent_info:known_to_merchant_location, false)::boolean as known_to_merchant_location,

            $1:consent_info:tos_version:privacy_version::string as privacy_version,
            $1:consent_info:tos_version:terms_version::string as terms_version,
            $1:consent_info:tos_version:bundle_version::string as bundle_version,

            nvl($1:filter_tags:is_employee, false)::boolean as is_employee,
            nvl($1:stats:portal_blip_count, 0)::integer as portal_blip_count
        FROM ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS_ARCHIVE_CHANGES QUALIFY ROW_NUMBER() OVER (PARTITION BY sighting_id order by INSERT_ID desc) = 1
    ) as wfs_changes on wfs.sighting_id = wfs_changes.sighting_id
    WHEN MATCHED THEN 
        UPDATE SET
            classification = wfs_changes.classification,
            classification_reasons = wfs_changes.classification_reasons,
            start_time = wfs_changes.start_time,
            end_time = wfs_changes.end_time,
            blip_count = wfs_changes.blip_count,
            max_rssi = wfs_changes.max_rssi,
            min_rssi = wfs_changes.min_rssi,
            avg_rssi = wfs_changes.avg_rssi,
            client_mac_info = wfs_changes.client_mac_info,
            contact_id = wfs_changes.contact_id,
            contact_info = wfs_changes.contact_info,
            contact_method = wfs_changes.contact_method,
            location_id = wfs_changes.location_id,
            account_id = wfs_changes.account_id,
            known_to_zenreach = wfs_changes.known_to_zenreach,
            known_to_merchant_account = wfs_changes.known_to_merchant_account,
            known_to_merchant_location = wfs_changes.known_to_merchant_location,
            privacy_version = wfs_changes.privacy_version,
            terms_version = wfs_changes.terms_version,
            bundle_version = wfs_changes.bundle_version,
            is_employee = wfs_changes.is_employee,
            portal_blip_count = wfs_changes.portal_blip_count
    WHEN NOT MATCHED THEN 
        INSERT (
            sighting_id,
            classification,
            classification_reasons,
            start_time,
            end_time,
            blip_count,
            max_rssi,
            min_rssi,
            avg_rssi,
            client_mac_info,
            contact_id,
            contact_info,
            contact_method,
            location_id,
            account_id,
            known_to_zenreach,
            known_to_merchant_account,
            known_to_merchant_location,
            privacy_version,
            terms_version,
            bundle_version,
            is_employee,
            portal_blip_count
        ) VALUES (
            wfs_changes.sighting_id,
            wfs_changes.classification,
            wfs_changes.classification_reasons,
            wfs_changes.start_time,
            wfs_changes.end_time,
            wfs_changes.blip_count,
            wfs_changes.max_rssi,
            wfs_changes.min_rssi,
            wfs_changes.avg_rssi,
            wfs_changes.client_mac_info,
            wfs_changes.contact_id,
            wfs_changes.contact_info,
            wfs_changes.contact_method,
            wfs_changes.location_id,
            wfs_changes.account_id,
            wfs_changes.known_to_zenreach,
            wfs_changes.known_to_merchant_account,
            wfs_changes.known_to_merchant_location,
            wfs_changes.privacy_version,
            wfs_changes.terms_version,
            wfs_changes.bundle_version,
            wfs_changes.is_employee,
            wfs_changes.portal_blip_count
        );

ALTER TASK ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS_UPSERT_TASK RESUME;