-------------------------------------------------------------------
----------------- USAGE_BILLING_INFO_ACCOUNT_THROUGH table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.USAGE_BILLING_INFO_ACCOUNT_THROUGH_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.USAGE_BILLING_INFO_ACCOUNT_THROUGH as
          select
            $1::number as id,
            $2::number as usagebillinginfo_id,
            $3::number as account_id,
            current_timestamp() as of_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/usagebillinginfo_account_through.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.USAGE_BILLING_INFO_ACCOUNT_THROUGH_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.USAGE_BILLING_INFO_ACCOUNT_THROUGH_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.USAGE_BILLING_INFO_ACCOUNT_THROUGH_TASK resume;