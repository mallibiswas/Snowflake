-------------------------------------------------------------------
----------------- AMDASHBOARD_LAST_LOGGED_IN_USERS_SALESFORCE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.AMDASHBOARD_LAST_LOGGED_IN_USERS_SALESFORCE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.AMDASHBOARD_LAST_LOGGED_IN_USERS_SALESFORCE as
          select
            $1 as business_id,
            $2 as last_login_email_list,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_last_logged_in_users_salesforce.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.AMDASHBOARD_LAST_LOGGED_IN_USERS_SALESFORCE_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.AMDASHBOARD_LAST_LOGGED_IN_USERS_SALESFORCE_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.AMDASHBOARD_LAST_LOGGED_IN_USERS_SALESFORCE_TASK resume;