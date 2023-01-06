-------------------------------------------------------------------
----------------- AMDASHBOARD_DASHBOARD_LOGINS_RS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.AMDASHBOARD_DASHBOARD_LOGINS_RS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.AMDASHBOARD_DASHBOARD_LOGINS_RS as
          select
            $1 as business_id,
            $2::number as dashboard_days_accessed_count,
            $3::timestamp as dashboard_first_access_date,
            $4::timestamp as dashboard_last_access_date,
            $5 as parent_business_id,
            $6::number as parent_days_accessed_count,
            $7::timestamp as parent_first_access_date,
            $8::timestamp as parent_last_access_date,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_dashboard_logins_rs.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.AMDASHBOARD_DASHBOARD_LOGINS_RS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.AMDASHBOARD_DASHBOARD_LOGINS_RS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.AMDASHBOARD_DASHBOARD_LOGINS_RS_TASK resume;