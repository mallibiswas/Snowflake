create file format if not exists  s3_parquet_format
  type = 'PARQUET'
  snappy_compression = true
  BINARY_AS_TEXT = false
  strip_outer_array = true;


create or replace stage s3_dashboard_events_stage
  file_format = s3_parquet_format
--  url = 's3://zp-uw2-redshift-loader-bizint/dashboard_events/' -- old
  url = 's3://zp-uw2-platform-kafka-archives/secor/dashboard_events_0/' -- New 2/11
  credentials = (aws_key_id='************' aws_secret_key='*********************');



show stages
show pipes


-- load history
COPY INTO zenprod.crm.dashboard_events (uuid,
        timestamp,
        page,
        event_name,
        event_type,
        platform,
        browser,
        browser_version,
        user_id,
        email,
        business_id)
FROM
      (select parse_json($1):id::string, -- uuid
       to_timestamp(parse_json($1):timestamp:seconds), -- timestamp
       parse_json($1):page::string, -- page 
       parse_json($1):event::string, -- event name
       parse_json($1):event_type::string, -- event name
       parse_json($1):platform::string, -- platform
       parse_json($1):browser::string, -- browser
       parse_json($1):browser_version::string, -- browser_version
       parse_json($1):user_id::string, -- user_id
       parse_json($1):email::string, -- email
       parse_json($1):business_id::string -- business_id               
       from @s3_dashboard_events_stage
      )
--pattern = '.*.csv'
on_error = 'continue'
;

-- Turn pipe on
CREATE OR REPLACE pipe zenprod._stage.dashboard_events_snowpipe auto_ingest=true as
COPY INTO zenprod.crm.dashboard_events (uuid,
        timestamp,
        page,
        event_name,
        event_type,
        platform,
        browser,
        browser_version,
        user_id,
        email,
        business_id)
FROM
      (select parse_json($1):id::string, -- uuid
       to_timestamp(parse_json($1):timestamp:seconds), -- timestamp
       parse_json($1):page::string, -- page 
       parse_json($1):event::string, -- event name
       parse_json($1):event_type::string, -- event name
       parse_json($1):platform::string, -- platform
       parse_json($1):browser::string, -- browser
       parse_json($1):browser_version::string, -- browser_version
       parse_json($1):user_id::string, -- user_id
       parse_json($1):email::string, -- email
       parse_json($1):business_id::string -- business_id               
       from @s3_dashboard_events_stage
      )
--pattern = '.*.csv'
on_error = 'continue'
;

