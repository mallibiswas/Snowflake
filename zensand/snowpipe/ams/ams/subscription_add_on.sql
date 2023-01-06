-------------------------------------------------------------------
----------------- SUBSCRIPTION_ADD_ON table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.SUBSCRIPTION_ADD_ON_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.SUBSCRIPTION_ADD_ON as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as subscription_id,
            $5::number as plan_addon_id,
            $6::number as qty,
            $7::number as subscription_addon_price_id,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/subscriptionaddon.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.SUBSCRIPTION_ADD_ON_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.SUBSCRIPTION_ADD_ON_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.SUBSCRIPTION_ADD_ON_TASK resume;