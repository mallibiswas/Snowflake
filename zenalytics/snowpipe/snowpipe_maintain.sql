-- use correct roles/schemas to view ingestions
use role sysadmin;
use schema _staging;

-- check last loads
select * from information_schema.load_history
  order by last_load_time desc
  limit 10;
 
--- check active snowpipes
show pipes;

-- Run/Pause all running pipes contained in the schema (if needed)
alter schema zenalytics._staging set pipe_execution_paused=false;

-- Run/Pause specific running pipe contained in the schema
alter pipe zenalytics._staging.SIGHTINGS_SNOWPIPE set pipe_execution_paused=false;
alter pipe zenalytics._staging.ENRICHED_SIGHTINGS_SNOWPIPE set pipe_execution_paused=True;
alter pipe zenalytics._staging.VISIT_STATS_SNOWPIPE set pipe_execution_paused=True;
alter pipe zenalytics._staging.WALKTHROUGH_SNOWPIPE set pipe_execution_paused=True;


-- check snowpipe status
select system$pipe_status('zenalytics._staging.VISITS_SNOWPIPE'); 
select system$pipe_status('zenalytics._staging.ADS_SNOWPIPE'); 
select system$pipe_status('zenalytics._staging.PORTAL_EVENTS_SNOWPIPE'); 
select system$pipe_status('zenalytics._staging.DASHBOARD_EVENTS_SNOWPIPE');
select system$pipe_status('zenalytics._staging.ENRICHED_SIGHTINGS_SNOWPIPE');
select system$pipe_status('zenalytics._staging.PORTAL_BLIPS_SNOWPIPE');
select system$pipe_status('zenalytics._staging.LOCATION_BLIPS_SNOWPIPE');
select system$pipe_status('zenalytics._staging.CONSENTED_SIGHTINGS_SNOWPIPE');

-- deprecated 6/27 based on conversation with Mark G. around GDPR compliance
select system$pipe_status('zenalytics._staging.MAIL_HOOK_SNOWPIPE'); 
select system$pipe_status('zenalytics._staging.MACTOCONTACTMAPPINGS_SNOWPIPE'); 
select system$pipe_status('ZENALYTICS._STAGING.anonymizer_snowpipe'); 
select system$pipe_status('zenalytics._staging.SIGHTINGS_SNOWPIPE'); 
select system$pipe_status('zenalytics._staging.VISIT_STATS_SNOWPIPE');
select system$pipe_status('zenalytics._staging.WALKTHROUGH_SNOWPIPE');

-- delete deprecated snowpipes
drop pipe zenalytics._staging.MAIL_HOOK_SNOWPIPE;
drop pipe zenalytics._staging.MACTOCONTACTMAPPINGS_SNOWPIPE;
drop pipe ZENALYTICS._STAGING.anonymizer_snowpipe;
drop pipe zenalytics._staging.SIGHTINGS_SNOWPIPE;
drop pipe zenalytics._staging.VISIT_STATS_SNOWPIPE;
drop pipe zenalytics._staging.WALKTHROUGH_SNOWPIPE;

-----------------------------
------Check load history
-----------------------------

select * from table(information_schema.copy_history(table_name=>'GENERATOR_WALKTHROUGH', start_time=> dateadd(hours, -1, current_timestamp())));
select * from table(information_schema.copy_history(table_name=>'PYCONVERSIONS_WALKTHROUGH', start_time=> dateadd(hours, -1, current_timestamp())));

-----------------------------
------SETUP SNOWPIPE ROLE
-----------------------------

CREATE or REPLACE ROLE snowpipe_role COMMENT = 'Role for Snowpipe continuous loading';
GRANT ROLE SNOWPIPE_ROLE to role SYSADMIN;
GRANT ALL ON WAREHOUSE ZENSYNCER TO ROLE SNOWPIPE_ROLE;
GRANT ALL ON DATABASE ZENALYTICS TO ROLE SNOWPIPE_ROLE;
grant all on schema ZENALYTICS.PRESENCE to role SNOWPIPE_ROLE;
grant all on schema ZENALYTICS._STAGING to role SNOWPIPE_ROLE;


