-------------------------------------------------------------------
----------------- QUOTA_TYPE table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_PRODUCTLICENSER.QUOTA_TYPE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_PRODUCTLICENSER.QUOTA_TYPE as
          select
            $1 as id,
            $2 as code,
            $3 as product_id,
            $4::timestamp as created,
            current_timestamp() as of_date
          FROM @ZENPROD.AMS_PRODUCTLICENSER.AMS_PRODUCTLICENSER_S3_STAGE/${FILE_DATE}/quota_type.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_PRODUCTLICENSER.QUOTA_TYPE_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_PRODUCTLICENSER.QUOTA_TYPE_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_PRODUCTLICENSER.QUOTA_TYPE_TASK resume;
