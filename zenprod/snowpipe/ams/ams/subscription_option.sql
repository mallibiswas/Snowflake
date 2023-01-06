-------------------------------------------------------------------
----------------- SUBSCRIPTION_OPTION table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.SUBSCRIPTION_OPTION_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.SUBSCRIPTION_OPTION as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4 as target_payment_term,
            $5 as target_renewal_term,
            $6 as source_payment_term,
            $7 as source_renewal_term,
            $8::number as fee_discount,
            $9 as group_code,
            $10::number as target_plan_id,
            $11::number as source_plan_id,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/subscriptionoption.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.SUBSCRIPTION_OPTION_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.SUBSCRIPTION_OPTION_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.SUBSCRIPTION_OPTION_TASK resume;