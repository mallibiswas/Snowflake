-------------------------------------------------------------------
----------------- SUBSCRIPTIONS_V2_LOCATION_THROUGH table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.SUBSCRIPTIONS_V2_LOCATION_THROUGH_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.SUBSCRIPTIONS_V2_LOCATION_THROUGH as
          select
            $1::number as id,
            $2::number as subscriptionv2_id,
            $3::number as location_id,
            current_timestamp() as of_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/subscriptions_v2_location_through.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.SUBSCRIPTIONS_V2_LOCATION_THROUGH_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.SUBSCRIPTIONS_V2_LOCATION_THROUGH_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.SUBSCRIPTIONS_V2_LOCATION_THROUGH_TASK resume;