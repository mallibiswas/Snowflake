-------------------------------------------------------------------
----------------- AMDASHBOARD_SMART_MESSAGES_CONFIG table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.AMDASHBOARD_SMART_MESSAGES_CONFIG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.AMDASHBOARD_SMART_MESSAGES_CONFIG as
          select
            $1 as business_id,
            $2::number as smart_messages_configured_count,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_smart_messages_config.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.AMDASHBOARD_SMART_MESSAGES_CONFIG_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.AMDASHBOARD_SMART_MESSAGES_CONFIG_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.AMDASHBOARD_SMART_MESSAGES_CONFIG_TASK resume;