-------------------------------------------------------------------
----------------- REPMANAGEMENT_SETTINGS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.REPMANAGEMENT_SETTINGS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.REPMANAGEMENT_SETTINGS as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'created:$date')::timestamp as created,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:facebook_url_enabled::boolean as facebook_url_enabled,
      $1:tripadvisor_url_enabled::boolean as tripadvisor_url_enabled,
      $1:yelp_url_enabled::boolean as yelp_url_enabled,
      $1:opentable_url_enabled::boolean as opentable_url_enabled,
      $1:google_url_enabled::boolean as google_url_enabled,
      $1:facebook_url::string as facebook_url,
      $1:google_url::string as google_url,
      $1:tripadvisor_url::string as tripadvisor_url,
      $1:opentable_url::string as opentable_url,
      $1:logo_url::string as logo_url,
      $1:yelp_url::string as yelp_url,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/repmanagement_settings.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.REPMANAGEMENT_SETTINGS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.REPMANAGEMENT_SETTINGS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.REPMANAGEMENT_SETTINGS_TASK resume;
