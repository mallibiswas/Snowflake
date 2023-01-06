-------------------------------------------------------------------
----------------- AMDASHBOARD_REP_MANAGEMENT_SF table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.AMDASHBOARD_REP_MANAGEMENT_SF_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.AMDASHBOARD_REP_MANAGEMENT_SF as
          select
            $1 as business_id,
            $2::number as rep_mgmt_ratings_count,
            $3::number as rep_mgmt_template_count,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_rep_management_sf.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.AMDASHBOARD_REP_MANAGEMENT_SF_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.AMDASHBOARD_REP_MANAGEMENT_SF_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.AMDASHBOARD_REP_MANAGEMENT_SF_TASK resume;