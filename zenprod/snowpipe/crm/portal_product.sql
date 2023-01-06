-------------------------------------------------------------------
----------------- PORTAL_PRODUCT table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.PORTAL_PRODUCT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.PORTAL_PRODUCT as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'date_added:$date')::timestamp as date_added,
      $1:name::string as name,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_product.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.PORTAL_PRODUCT_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.PORTAL_PRODUCT_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.PORTAL_PRODUCT_TASK resume;
