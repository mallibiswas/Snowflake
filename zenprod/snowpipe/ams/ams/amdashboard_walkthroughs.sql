-------------------------------------------------------------------
----------------- AMDASHBOARD_WALKTHROUGHS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.AMDASHBOARD_WALKTHROUGHS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.AMDASHBOARD_WALKTHROUGHS as
          select
            $1 as business_id,
            $2::number as walkthrough_past_thirty,
            $3::number as walkthrough_total,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_walkthroughs.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.AMDASHBOARD_WALKTHROUGHS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.AMDASHBOARD_WALKTHROUGHS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.AMDASHBOARD_WALKTHROUGHS_TASK resume;