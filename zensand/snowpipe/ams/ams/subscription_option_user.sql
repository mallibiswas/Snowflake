-------------------------------------------------------------------
----------------- SUBSCRIPTION_OPTION_USER table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.SUBSCRIPTION_OPTION_USER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.SUBSCRIPTION_OPTION_USER as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4 as group_code,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/subscriptionoptionuser.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.SUBSCRIPTION_OPTION_USER_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.SUBSCRIPTION_OPTION_USER_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.SUBSCRIPTION_OPTION_USER_TASK resume;