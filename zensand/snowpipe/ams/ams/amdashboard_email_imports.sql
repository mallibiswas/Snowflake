-------------------------------------------------------------------
----------------- AMDASHBOARD_EMAIL_IMPORTS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.AMDASHBOARD_EMAIL_IMPORTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.AMDASHBOARD_EMAIL_IMPORTS as
          select
            $1 as business_id,
            $2::number as contact_list_import_failure_count,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_email_imports.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.AMDASHBOARD_EMAIL_IMPORTS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.AMDASHBOARD_EMAIL_IMPORTS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.AMDASHBOARD_EMAIL_IMPORTS_TASK resume;