-------------------------------------------------------------------
----------------- AMDASHBOARD_SMART_MESSAGE_USAGE_SF table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.AMDASHBOARD_SMART_MESSAGE_USAGE_SF_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.AMDASHBOARD_SMART_MESSAGE_USAGE_SF as
          select
            $1 as business_id,
            $2::number as total_smart_messages_sent_past_thirty,
            $3::number as total_blast_messages_sent_past_thirty,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_smart_message_usage_sf.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.AMDASHBOARD_SMART_MESSAGE_USAGE_SF_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.AMDASHBOARD_SMART_MESSAGE_USAGE_SF_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.AMDASHBOARD_SMART_MESSAGE_USAGE_SF_TASK resume;