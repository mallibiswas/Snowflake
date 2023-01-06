-------------------------------------------------------------------
----------------- ROUTER table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_ROUTERS.ROUTER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_ROUTERS.ROUTER as
          select
            $1 as id,
            $2 as account_id,
            $3 as mac_start,
            $4 as mac_end,
            $5 as router_type,
            $6::timestamp as created,
            $7::timestamp as updated,
            $8::timestamp as deleted,
            $9 as node_id,
            $10 as serial_number,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS_ROUTERS.AMS_ROUTERS_S3_STAGE/${FILE_DATE}/router.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_ROUTERS.ROUTER_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_ROUTERS.ROUTER_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_ROUTERS.ROUTER_TASK resume;