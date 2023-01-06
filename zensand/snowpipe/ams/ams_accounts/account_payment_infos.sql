-------------------------------------------------------------------
----------------- account_payment_infos table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS_ACCOUNTS.ACCOUNT_PAYMENT_INFOS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS_ACCOUNTS.ACCOUNT_PAYMENT_INFOS as
          select
            $1 as id,
            $2 as payment_info_id,
            $3 as account_id,
            $4::timestamp as created,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/account_payment_infos.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS_ACCOUNTS.ACCOUNT_PAYMENT_INFOS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS_ACCOUNTS.ACCOUNT_PAYMENT_INFOS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS_ACCOUNTS.ACCOUNT_PAYMENT_INFOS_TASK resume;