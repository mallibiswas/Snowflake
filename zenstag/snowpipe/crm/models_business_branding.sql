-------------------------------------------------------------------
----------------- MODELS_BUSINESS_BRANDING table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.MODELS_BUSINESS_BRANDING_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.MODELS_BUSINESS_BRANDING as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:button_color::string as button_color,
      $1:font::string as font,
      GET_PATH($1, 'created:$date')::timestamp as created,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:logo_id::string as logo_id,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/models_businessbranding.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.MODELS_BUSINESS_BRANDING_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.MODELS_BUSINESS_BRANDING_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.MODELS_BUSINESS_BRANDING_TASK resume;
