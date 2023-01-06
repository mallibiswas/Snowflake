-------------------------------------------------------------------
----------------- REPMANAGEMENT_SETTINGS table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.REPMANAGEMENT_SETTINGS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.REPMANAGEMENT_SETTINGS as
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
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/repmanagement_settings.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.REPMANAGEMENT_SETTINGS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.REPMANAGEMENT_SETTINGS_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.REPMANAGEMENT_SETTINGS_TASK resume;
