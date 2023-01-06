-------------------------------------------------------------------
----------------- SALESFORCE_ACCOUNT table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS_ACCOUNTS.SALESFORCE_ACCOUNT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS_ACCOUNTS.SALESFORCE_ACCOUNT as
          select
            $1 as id,
            $2 as name,
            $3::timestamp as created,
            $4::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/salesforce_account.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS_ACCOUNTS.SALESFORCE_ACCOUNT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS_ACCOUNTS.SALESFORCE_ACCOUNT_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS_ACCOUNTS.SALESFORCE_ACCOUNT_TASK resume;