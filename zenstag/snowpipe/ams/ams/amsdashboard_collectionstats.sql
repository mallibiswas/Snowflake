-------------------------------------------------------------------
----------------- AMDASHBOARD_COLLECTIONSTATS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.AMDASHBOARD_COLLECTIONSTATS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.AMDASHBOARD_COLLECTIONSTATS as
          select
            $1 as business_id,
            $2::number as average_emails_collected_all_time,
            $3::number as total_emails_collected_all_time,
            $4::number as total_emails_collected_past_thirty,
            $5::number as average_emails_collected_past_thirty,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_collectionstats.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.AMDASHBOARD_COLLECTIONSTATS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.AMDASHBOARD_COLLECTIONSTATS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.AMDASHBOARD_COLLECTIONSTATS_TASK resume;