-------------------------------------------------------------------
----------------- CHARGE_LOG table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_ACCOUNTS.CHARGE_LOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_ACCOUNTS.CHARGE_LOG as
          select
            $1 as id,
            $2 as account_id,
            $3 as salesforce_quote_line_item,
            $4 as charge_id,
            $5::number as unit_amount_in_cents,     
            $6::number as quantitity,
            $7 as description,
            $8 as error,
            $9::timestamp as created,
            $10::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/charge_log.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_ACCOUNTS.CHARGE_LOG_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_ACCOUNTS.CHARGE_LOG_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_ACCOUNTS.CHARGE_LOG_TASK resume;