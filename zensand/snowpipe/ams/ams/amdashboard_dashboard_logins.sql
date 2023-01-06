-------------------------------------------------------------------
----------------- AMDASHBOARD_DASHBOARD_LOGINS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.AMDASHBOARD_DASHBOARD_LOGINS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.AMDASHBOARD_DASHBOARD_LOGINS as
          select
            $1 as business_id,
            $2::number as dashboard_days_accessed_count,
            $3::timestamp as dashboard_first_access_date,
            $4::timestamp as dashboard_last_access_date,
            $5::timestamp as parent_last_access_date,
            $6 as parent_business_id,
            $7::number as parent_days_accessed_count,
            $8::timestamp as parent_first_access_date,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_dashboard_logins.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.AMDASHBOARD_DASHBOARD_LOGINS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.AMDASHBOARD_DASHBOARD_LOGINS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.AMDASHBOARD_DASHBOARD_LOGINS_TASK resume;