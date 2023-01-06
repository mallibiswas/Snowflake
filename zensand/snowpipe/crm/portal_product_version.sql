-------------------------------------------------------------------
----------------- PORTAL_PRODUCT_VERSION table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.PORTAL_PRODUCT_VERSION_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.PORTAL_PRODUCT_VERSION as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:name::integer as name,
      $1:label::string as label,
      GET_PATH($1, 'date_added:$date')::timestamp as date_added,
      GET_PATH($1, 'product_id:$oid')::string as product_id,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_productversion.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.PORTAL_PRODUCT_VERSION_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.PORTAL_PRODUCT_VERSION_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.PORTAL_PRODUCT_VERSION_TASK resume;


