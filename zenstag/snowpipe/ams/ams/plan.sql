-------------------------------------------------------------------
----------------- PLAN table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.PLAN_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.PLAN as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4 as plan_code,
            $5 as payment_term,
            $6 as name,
            $7 as contract_template,
            $8::boolean as has_trial,
            $9 as product_code,
            $10::number as product_id,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/plan.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.PLAN_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.PLAN_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.PLAN_TASK resume;