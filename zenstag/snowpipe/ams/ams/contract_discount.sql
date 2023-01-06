-------------------------------------------------------------------
----------------- CONTRACT_DISCOUNT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.CONTRACT_DISCOUNT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.CONTRACT_DISCOUNT as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4 as discount_id,
            $5::number as contract_id,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/contractdiscount.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.CONTRACT_DISCOUNT_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.CONTRACT_DISCOUNT_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.CONTRACT_DISCOUNT_TASK resume;