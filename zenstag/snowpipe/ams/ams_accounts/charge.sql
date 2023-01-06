-------------------------------------------------------------------
----------------- CHARGE table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS_ACCOUNTS.CHARGE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS_ACCOUNTS.CHARGE as
          select
            $1 as id,
            $2 as account_id,
            $3 as charge_id,
            $4 as name,
            $5::number as quantity,     
            $6::number as unit_price_cents,
            $7::timestamp as created,
            $8::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/charge.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS_ACCOUNTS.CHARGE_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS_ACCOUNTS.CHARGE_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS_ACCOUNTS.CHARGE_TASK resume;