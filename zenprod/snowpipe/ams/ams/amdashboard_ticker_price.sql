-------------------------------------------------------------------
----------------- AMDASHBOARD_TICKET_PRICE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.AMDASHBOARD_TICKET_PRICE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.AMDASHBOARD_TICKET_PRICE as
          select
            $1 as business_id,
            $2::number as walkthrough_ticket_price,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_ticket_price.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.AMDASHBOARD_TICKET_PRICE_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.AMDASHBOARD_TICKET_PRICE_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.AMDASHBOARD_TICKET_PRICE_TASK resume;