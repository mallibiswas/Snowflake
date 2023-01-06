-------------------------------------------------------------------
----------------- ROUTER table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.ROUTER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.ROUTER as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as account_id,
            $5::number as location_id,
            $6 as business_profile_id,
            $7 as router_id,
            $8 as mac,
            $9 as router_state,
            $10::timestamp as installed,
            $11 as shipped,
            $12 as delivered,
            $13 as type,
            $14::variant as vendor_data,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/router.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.ROUTER_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.ROUTER_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.ROUTER_TASK resume;