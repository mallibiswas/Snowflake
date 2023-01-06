-------------------------------------------------------------------
----------------- SUBSCRIPTION table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS_ACCOUNTS.SUBSCRIPTION_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS_ACCOUNTS.SUBSCRIPTION as
          select
            $1 as id,
            $2 as account_id,
            $3 as recurly_subscription_id,
            $4 as provider_type,
            $5 as product,
            $6 as package,
            $7::boolean as manual_invoice,
            $8::timestamp as created,
            $9::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/subscription.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS_ACCOUNTS.SUBSCRIPTION_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS_ACCOUNTS.SUBSCRIPTION_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS_ACCOUNTS.SUBSCRIPTION_TASK resume;