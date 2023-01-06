---------------------------------------------------------------------------------------
------- Walkthroughs
---------------------------------------------------------------------------------------

create table ZENALYTICs.PRESENCE.walkthroughs clone ZENDEV.ENGG.generator_walkthrough;

create or replace table ZENALYTICs.PRESENCE.walkthroughs 
as
SELECT 
    lower($1:user_id)::string as userprofile_id,
    lower($1:root_business_id)::string as root_business_id,
    $1:created_timestamp:seconds::integer as created_timestamp,    
    lower($1:contact_event:contacting_business_id)::string as contacting_business_id,
    $1:contact_event:mail_sent_timestamp:seconds::integer as mail_sent_timestamp,
    $1:contact_event:messagetype:value::string as messagetype,
    $1:contact_event:deliverytype:value::string as deliverytype,
    $1:contact_event:provider:value::string as provider,
    $1:contact_event:recipient:value::string as recipient,
    $1:contact_event:arguments::variant as arguments,
    $1:visit_event:visit_end_timestamp:seconds::integer as visit_end_timestamp,
    lower(substr($1:visit_event:client_mac::string,1,2)||':'||
    substr($1:visit_event:client_mac::string,3,2)||':'||
    substr($1:visit_event:client_mac::string,5,2)||':'||
    substr($1:visit_event:client_mac::string,7,2)||':'||
    substr($1:visit_event:client_mac::string,9,2)||':'||
    substr($1:visit_event:client_mac::string,11,2)) as client_mac,
    $1:visit_event:contact_info:value::string as contact_info,
    $1:visit_event:contact_method:value::string as contact_method,
    $1:visit_event:in_business_network:value::boolean as in_business_network,
    $1:visit_event:is_first_contact:value::boolean as is_first_contact,
    $1:visit_event:status:value::string as status,
    lower($1:visit_event:visited_business_id)::string as visited_business_id,    
    $1:source::string as source
FROM @s3_walkthrough_stage/date=2019-05-07/hour=20/1_9_00000000000000124117;

truncate table ZENALYTICs.PRESENCE.walkthroughs

-- alter table ZENALYTICs.PRESENCE.walkthroughs rename column user_id to userprofile_id;


create or replace stage zenalytics._staging.s3_walkthrough_stage
  file_format = s3_parquet_format
  url = 's3://zp-uw2-foundation-kafka-archives/secor/walkthroughsgenerator_walkthrough_bywalkthroughkey_0/'  
credentials = (aws_key_id='**************' aws_secret_key='***************');

create or replace pipe ZENALYTICS._STAGING.walkthrough_snowpipe auto_ingest=true as
COPY INTO ZENALYTICS.PRESENCE.walkthroughs
FROM
(
SELECT 
    lower($1:user_id)::string as userprofile_id,
    lower($1:root_business_id)::string as root_business_id,
    $1:created_timestamp:seconds::integer as created_timestamp,    
    lower($1:contact_event:contacting_business_id)::string as contacting_business_id,
    $1:contact_event:mail_sent_timestamp:seconds::integer as mail_sent_timestamp,
    $1:contact_event:messagetype:value::string as messagetype,
    $1:contact_event:deliverytype:value::string as deliverytype,
    $1:contact_event:provider:value::string as provider,
    $1:contact_event:recipient:value::string as recipient,
    $1:contact_event:arguments::variant as arguments,
    $1:visit_event:visit_end_timestamp:seconds::integer as visit_end_timestamp,
    lower(substr($1:visit_event:client_mac::string,1,2)||':'||
    substr($1:visit_event:client_mac::string,3,2)||':'||
    substr($1:visit_event:client_mac::string,5,2)||':'||
    substr($1:visit_event:client_mac::string,7,2)||':'||
    substr($1:visit_event:client_mac::string,9,2)||':'||
    substr($1:visit_event:client_mac::string,11,2)) as client_mac,
    $1:visit_event:contact_info:value::string as contact_info,
    $1:visit_event:contact_method:value::string as contact_method,
    $1:visit_event:in_business_network:value::boolean as in_business_network,
    $1:visit_event:is_first_contact:value::boolean as is_first_contact,
    $1:visit_event:status:value::string as status,
    lower($1:visit_event:visited_business_id)::string as visited_business_id,    
    $1:source::string as source
FROM @s3_walkthrough_stage
)
on_error = 'continue'
;

--- historical load
COPY INTO ZENALYTICS.PRESENCE.walkthroughs
FROM
(SELECT
    $1:user_id::string as userprofile_id,
    $1:root_business_id::string as root_business_id,
    $1:created_timestamp:seconds::integer as created_timestamp,    
    $1:contact_event:contacting_business_id::string as contacting_business_id,
    $1:contact_event:mail_sent_timestamp:seconds::integer as mail_sent_timestamp,
    $1:contact_event:messagetype:value::string as messagetype,
    $1:contact_event:deliverytype:value::string as deliverytype,
    $1:contact_event:provider:value::string as provider,
    $1:contact_event:recipient:value::string as recipient,
    $1:contact_event:arguments::variant as contact_events,
    $1:visit_event:visit_end_timestamp:seconds::integer as visit_end_timestamp,
    $1:visit_event:client_mac::string as client_mac,
    $1:visit_event:contact_info:value::string as contact_info,
    $1:visit_event:contact_method:value::string as contact_method,
    $1:visit_event:in_business_network:value::boolean as in_business_network,
    $1:visit_event:is_first_contact:value::boolean as is_first_contact,
    $1:visit_event:visited_business_id::string as visited_business_id,    
    $1:source::string::string as source
FROM @s3_walkthrough_stage
)
pattern = '.*\/date=2019-04-[0-9][0-9]\/hour=[0-2][0-9]\/.*'
on_error = 'continue'
;

