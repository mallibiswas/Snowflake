-------------------------------------------------------------------
----------------- SUBSCRIPTION table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.SUBSCRIPTION_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.SUBSCRIPTION as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as account_id,
            $5::number as billing_account_id,
            $6 as reculry_subscription,
            $7 as subscription_state,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/subscription.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.SUBSCRIPTION_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.SUBSCRIPTION_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.SUBSCRIPTION_TASK resume;