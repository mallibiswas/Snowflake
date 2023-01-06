-------------------------------------------------------------------
----------------- SUBSCRIPTION_SNAPSHOT table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS_ACCOUNTS.SUBSCRIPTION_SNAPSHOT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS_ACCOUNTS.SUBSCRIPTION_SNAPSHOT as
          select
            $1 as id,
            $2 as subscription_id,
            $3 as recurly_subscription_snapshot_id,
            $4 as account_id,
            $5 as recurly_subscription_id,
            $6 as provider_type,
            $7 as product,
            $8 as package,
            $9::boolean as manual_invoice,
            $10::timestamp as created,
            $11::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/subscription_snapshot.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS_ACCOUNTS.SUBSCRIPTION_SNAPSHOT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS_ACCOUNTS.SUBSCRIPTION_SNAPSHOT_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS_ACCOUNTS.SUBSCRIPTION_SNAPSHOT_TASK resume;