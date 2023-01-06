-------------------------------------------------------------------
----------------- AMDASHBOARD_ACCOUNT_MANAGERS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.AMDASHBOARD_ACCOUNT_MANAGERS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.AMDASHBOARD_ACCOUNT_MANAGERS as
          select
            $1 as business_id,
            $2 as salesforce_id,
            $3 as account_manager_email,
            $4 as account_manager,
            $5 as account_manager_manager,
            $6::timestamp as last_activity_date,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_account_managers.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.AMDASHBOARD_ACCOUNT_MANAGERS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.AMDASHBOARD_ACCOUNT_MANAGERS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.AMDASHBOARD_ACCOUNT_MANAGERS_TASK resume;