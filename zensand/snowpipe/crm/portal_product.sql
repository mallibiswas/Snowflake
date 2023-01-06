-------------------------------------------------------------------
----------------- PORTAL_PRODUCT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.PORTAL_PRODUCT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.PORTAL_PRODUCT as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'date_added:$date')::timestamp as date_added,
      $1:name::string as name,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_product.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.PORTAL_PRODUCT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.PORTAL_PRODUCT_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.PORTAL_PRODUCT_TASK resume;
