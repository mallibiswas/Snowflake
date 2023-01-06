-------------------------------------------------------------------
----------------- LICENSE table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_PRODUCTLICENSER.LICENSE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_PRODUCTLICENSER.LICENSE as
          select
            $1 as id,
            $2 as account_id,
            $3 as package_id,
            $4::number as total_units,
            $5::number as assigned_units,
            $6::timestamp as created,
            $7::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS_PRODUCTLICENSER.AMS_PRODUCTLICENSER_S3_STAGE/${FILE_DATE}/license.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_PRODUCTLICENSER.LICENSE_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_PRODUCTLICENSER.LICENSE_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_PRODUCTLICENSER.LICENSE_TASK resume;
