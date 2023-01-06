-------------------------------------------------------------------
----------------- AMDASHBOARD_EVENT_LOG table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.AMDASHBOARD_EVENT_LOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.AMDASHBOARD_EVENT_LOG as
          select
            $1 as id,
            $2::timestamp as created,
            $3 as type,
            $4 as details,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_event_logs.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.AMDASHBOARD_EVENT_LOG_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.AMDASHBOARD_EVENT_LOG_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.AMDASHBOARD_EVENT_LOG_TASK resume;