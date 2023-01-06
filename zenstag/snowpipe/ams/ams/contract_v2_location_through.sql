-------------------------------------------------------------------
----------------- CONTRACT_V2_LOCATION_THROUGH table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.CONTRACT_V2_LOCATION_THROUGH_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.CONTRACT_V2_LOCATION_THROUGH as
          select
            $1::number as id,
            $2::number as contractv2_id,
            $3::number as location_id,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/contract_v2_location_through.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.CONTRACT_V2_LOCATION_THROUGH_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.CONTRACT_V2_LOCATION_THROUGH_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.CONTRACT_V2_LOCATION_THROUGH_TASK resume;