-------------------------------------------------------------------
----------------- MODELS_BUSINESS_BRANDING table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.MODELS_BUSINESS_BRANDING_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.MODELS_BUSINESS_BRANDING as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:button_color::string as button_color,
      $1:font::string as font,
      GET_PATH($1, 'created:$date')::timestamp as created,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:logo_id::string as logo_id,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/models_businessbranding.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.MODELS_BUSINESS_BRANDING_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.MODELS_BUSINESS_BRANDING_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.MODELS_BUSINESS_BRANDING_TASK resume;
