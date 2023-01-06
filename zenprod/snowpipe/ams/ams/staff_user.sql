-------------------------------------------------------------------
----------------- STAFF_USER table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.STAFF_USER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.STAFF_USER as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4 as email,
            $5 as name,
            $6::timestamp as start_date,
            $7::timestamp as end_date,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/staffuser.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.STAFF_USER_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.STAFF_USER_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.STAFF_USER_TASK resume;