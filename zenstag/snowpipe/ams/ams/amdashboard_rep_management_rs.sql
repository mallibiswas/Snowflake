-------------------------------------------------------------------
----------------- AMDASHBOARD_REP_MANAGEMENT_RS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.AMDASHBOARD_REP_MANAGEMENT_RS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.AMDASHBOARD_REP_MANAGEMENT_RS as
          select
            $1 as business_id,
            $2::number as rep_mgmt_ratings_count,
            $3::number as rep_mgmt_template_count,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_rep_management_rs.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.AMDASHBOARD_REP_MANAGEMENT_RS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.AMDASHBOARD_REP_MANAGEMENT_RS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.AMDASHBOARD_REP_MANAGEMENT_RS_TASK resume;