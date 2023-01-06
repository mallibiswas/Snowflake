-------------------------------------------------------------------
----------------- PORTAL_PRODUCT_VERSION table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.PORTAL_PRODUCT_VERSION_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.PORTAL_PRODUCT_VERSION as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:name::integer as name,
      $1:label::string as label,
      GET_PATH($1, 'date_added:$date')::timestamp as date_added,
      GET_PATH($1, 'product_id:$oid')::string as product_id,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_productversion.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.PORTAL_PRODUCT_VERSION_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.PORTAL_PRODUCT_VERSION_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.PORTAL_PRODUCT_VERSION_TASK resume;


