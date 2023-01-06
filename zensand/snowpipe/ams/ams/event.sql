-------------------------------------------------------------------
----------------- EVENT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.EVENT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.EVENT as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as account_id,
            $5::number as contract_id,
            $6::number as location_id,
            $7::number as staff_user_id,
            $8::number as role_id,
            $9 as event_name,
            $10::timestamp as event_time,
            $11 as user_id,
            $12 as entity_name,
            $13::number as entity_id,
            $14::variant as payload
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/event.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.EVENT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.EVENT_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.EVENT_TASK resume;