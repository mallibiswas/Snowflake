-------------------------------------------------------------------
----------------- PAYMENT_INFO table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.PAYMENT_INFO_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.PAYMENT_INFO as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/payment_info.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.PAYMENT_INFO_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.PAYMENT_INFO_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.PAYMENT_INFO_TASK resume;