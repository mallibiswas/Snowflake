-------------------------------------------------------------------
----------------- account table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.ACCOUNT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.ACCOUNT as
          select
            $1 as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4 as name,
            $5 as salesforce_id,
            $6 as business_profile_id,
            $7 as account_state,
            $8::boolean as disqualified,
            $9 as billing_mode,
            $10 as partner_account_id,
            $11 as billing_account_id,
            $12::boolean as is_test,
            $13 as v3_account_id,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/account.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.ACCOUNT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.ACCOUNT_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.ACCOUNT_TASK resume;