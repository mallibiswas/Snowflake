-------------------------------------------------------------------
----------------- USAGE_BILLING_INFO table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.USAGE_BILLING_INFO_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.USAGE_BILLING_INFO as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4 as usage_type,
            $5::number as soft_limit,
            $6::number as hard_limit,
            $7::float as unit_cost_over_limit,
            current_timestamp() as of_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/usagebillinginfo.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.USAGE_BILLING_INFO_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.USAGE_BILLING_INFO_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.USAGE_BILLING_INFO_TASK resume;