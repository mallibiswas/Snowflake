--------------------------------------------------------------
--- portal events snowpipe
--------------------------------------------------------------
create or replace file format portal_csv_format
FIELD_DELIMITER=','
ESCAPE_UNENCLOSED_FIELD='\\'  
-- ESCAPE = '\\'
RECORD_DELIMITER='\n'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
EMPTY_FIELD_AS_NULL=true
null_if=('NULL') 
skip_header = 1;

create or replace stage s3_portal_events_stage
  file_format = portal_csv_format
  url = 's3://zp-uw2-redshift-loader-bizint-replica/portal_events/'
  credentials = (aws_key_id='**********' aws_secret_key='***********************');


CREATE OR REPLACE pipe zenalytics._staging.portal_events_snowpipe auto_ingest=true as
COPY INTO ZENALYTICS.CRM.PORTAL_EVENTS (UUID,
location_id,
session_id,
ap_mac,
client_mac,
ap_type,
created,
event_type,
event_context,
platform,
browser,
browser_version,
user_context)
FROM
(select $1 as UUID,
    $2 as location_id,
    $3 as session_id,
    $4 as ap_mac,
    $5 as client_mac,
    $6 as ap_type,  
    $7::timestamp as created, 
    $8 as event_type, 
    parse_json($9)::variant as event_context, 
    $10 as platform, 
    $11 as browser, 
    $12 as browser_version, 
    parse_json($13)::variant as user_context
from @s3_portal_events_stage
)
on_error = 'continue'
;

