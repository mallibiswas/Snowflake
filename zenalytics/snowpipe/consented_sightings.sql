-------------------------------------------------------------------
----------------- Consented Sightings Snowpipe
-------------------------------------------------------------------


create function if not exists string_to_mac(A string)
  returns string
  language javascript
as
$$
  return A.match(/.{1,2}/g).join( ':' );
$$
;

create or replace stage s3_consentedsightings_stage
  file_format = s3_parquet_format
  url = 's3://zp-uw2-foundation-kafka-archives/secor/consentedsightings_enrichedsighting_byclientmac_0/'
  credentials = (aws_key_id='************************' aws_secret_key='*********************');

create or replace pipe ZENALYTICS._STAGING.consented_sightings_snowpipe auto_ingest=true as
COPY INTO ZENALYTICS.PRESENCE.CONSENTED_SIGHTINGS
FROM
(
SELECT string_to_mac(lower(parse_json($1):client_mac::string)) as client_mac,
    parse_json($1):contact:created:seconds::integer as contact_created_date,
    parse_json($1):contact:id::string as contact_id,
    parse_json($1):contact:info::string as contact_info,
    parse_json($1):contact:method::string as contact_method,    
    parse_json($1):in_business_network::boolean as in_business_network,
    lower(parse_json($1):location_id::string) as business_id,
    parse_json($1):start_time::integer as start_time,
    parse_json($1):end_time::integer as end_time,
    parse_json($1):is_walk_in:value::boolean as is_walk_in,
    parse_json($1):source::variant as source,
    parse_json($1):stats:blip_count::integer as blip_count,
    parse_json($1):stats:max_rssi::float as max_rssi,
    parse_json($1):stats:min_rssi::float as min_rssi,
    parse_json($1):stats:avg_rssi::float as avg_rssi,
    nvl(parse_json($1):status,'NEW')::string as status
FROM @s3_consentedsightings_stage
)
on_error = 'continue'
;

