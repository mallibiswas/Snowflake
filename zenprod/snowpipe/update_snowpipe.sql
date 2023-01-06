-- Update ARCHIVER_WIFI_ENRICHED_SIGHTINGS_SNOWPIPE

-- pause pipe
alter pipe ZENPROD.PRESENCE.ARCHIVER_WIFI_ENRICHED_SIGHTINGS_SNOWPIPE set pipe_execution_paused = true;
-- ensure pending files is 0
select system$pipe_status('ZENPROD.PRESENCE.ARCHIVER_WIFI_ENRICHED_SIGHTINGS_SNOWPIPE'); 
-- re-create pipe to copy into the old table
create or replace pipe ZENPROD.PRESENCE.ARCHIVER_WIFI_ENRICHED_SIGHTINGS_SNOWPIPE auto_ingest=true as
COPY INTO ZENPROD.PRESENCE.WIFI_ENRICHED_SIGHTINGS
FROM (SELECT 
    HEX_ENCODE(BASE64_DECODE_BINARY($1:sighting_id)) as sighting_id,
    nvl($1:status,'Status_UNKNOWN')::string as status,

    TO_TIMESTAMP_NTZ($1:start_time) as start_time,
    TO_TIMESTAMP_NTZ($1:end_time) as end_time,

    nvl($1:stats:blip_count, 0)::integer as blip_count,
    nvl($1:stats:max_rssi, 0)::integer as max_rssi,
    nvl($1:stats:min_rssi, 0)::integer as min_rssi,
    nvl($1:stats:avg_rssi, 0)::float as avg_rssi,

    DECODE_CLIENT_MAC_INFO($1:client_mac_info::array) as client_mac_info,

    HEX_ENCODE(BASE64_DECODE_BINARY($1:contact_info:contact_id)) as contact_id,
    $1:contact_info:contact_info::string as contact_info,
    $1:contact_info:contact_method::string as contact_method,

    HEX_ENCODE(BASE64_DECODE_BINARY($1:location_info:location_id)) as location_id,
    HEX_ENCODE(BASE64_DECODE_BINARY($1:location_info:account_id)) as account_id,

    nvl($1:consent_info:known_to_zenreach, false)::boolean as known_to_zenreach,
    nvl($1:consent_info:known_to_merchant_account, false)::boolean as known_to_merchant_account,
    nvl($1:consent_info:known_to_merchant_location, false)::boolean as known_to_merchant_location,

    nvl($1:consent_info:tos_version:privacy_version::string, '') as privacy_version,
    nvl($1:consent_info:tos_version:terms_version::string, '') as terms_version,
    nvl($1:consent_info:tos_version:bundle_version::string, '') as bundle_version,


    nvl($1:filter_tags:is_employee, false)::boolean as is_employee,
    nvl($1:stats:portal_blip_count, 0)::integer as portal_blip_count

FROM @ZENPROD.PRESENCE.ARCHIVER_WIFI_ENRICHED_SIGHTINGS_S3_STAGE)
on_error = 'continue';
-- ensure pipe execution state is running
select system$pipe_status('ZENPROD.PRESENCE.ARCHIVER_WIFI_ENRICHED_SIGHTINGS_SNOWPIPE'); 

-- remove unnecessary new table
drop table if exists ZENPROD.PRESENCE.ARCHIVER_WIFI_ENRICHED_SIGHTINGS

-- Update ARCHIVER_WIFI_UNKNOWN_AP_BLIPS_SNOWPIPE

-- pause pipe
alter pipe ZENPROD.PRESENCE.ARCHIVER_WIFI_UNKNOWN_AP_BLIPS_SNOWPIPE set pipe_execution_paused = true;
-- ensure pending files is 0
select system$pipe_status('ZENPROD.PRESENCE.ARCHIVER_WIFI_UNKNOWN_AP_BLIPS_SNOWPIPE'); 
-- re-create pipe to copy into the old table
create or replace pipe ZENPROD.PRESENCE.ARCHIVER_WIFI_UNKNOWN_AP_BLIPS_SNOWPIPE auto_ingest=true as
COPY INTO ZENPROD.PRESENCE.WIFI_UNKNOWNAP_BLIP
FROM (SELECT 
    nvl($1:ap_type, 'APType_UNKNOWN')::string as ap_type,
    ZENPROD.PRESENCE.string_to_mac(HEX_ENCODE(BASE64_DECODE_BINARY($1:ap_mac::string))) as ap_mac,

    ZENPROD.PRESENCE.string_to_mac(HEX_ENCODE(BASE64_DECODE_BINARY($1:client_mac::string))) as client_mac,

    nvl($1:stats:rssi, 0)::integer as rssi,

    to_timestamp_ntz($1:server_ts:seconds::integer * 1000 + nvl($1:server_ts:nanos, 0)::integer / 1000000, 3) as server_ts,

    HEX_ENCODE(BASE64_DECODE_BINARY($1:client_mac_anonymization::string)) as client_mac_anonymization

FROM @ZENPROD.PRESENCE.ARCHIVER_WIFI_UNKNOWN_AP_BLIPS_S3_STAGE)
on_error = 'continue';
-- ensure pipe execution state is running
select system$pipe_status('ZENPROD.PRESENCE.ARCHIVER_WIFI_UNKNOWN_AP_BLIPS_SNOWPIPE'); 

-- remove unnecessary new table
drop table if exists ZENPROD.PRESENCE.ARCHIVER_WIFI_UNKNOWN_AP_BLIPS
