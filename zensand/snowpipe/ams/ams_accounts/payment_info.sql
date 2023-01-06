-------------------------------------------------------------------
----------------- PAYMENT_INFO table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS_ACCOUNTS.PAYMENT_INFO_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS_ACCOUNTS.PAYMENT_INFO as
          select
            $1 as id,
            $2 as provider_type,
            $3 as recurly_provider_id,
            $4::timestamp as created,
            $5::timestamp as updated,
            $6 as type,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/payment_info.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS_ACCOUNTS.PAYMENT_INFO_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS_ACCOUNTS.PAYMENT_INFO_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS_ACCOUNTS.PAYMENT_INFO_TASK resume;