-------------------------------------------------------------------
----------------- LOGICAL_ROUTER table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS_ROUTERS.LOGICAL_ROUTER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS_ROUTERS.LOGICAL_ROUTER as
          select
            $1 as id,
            $2 as router_id,
            $3 as mac,
            $4::timestamp as created_in_crm,
            $5::timestamp as created,
            $6::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS_ROUTERS.AMS_ROUTERS_S3_STAGE/${FILE_DATE}/logical_router.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS_ROUTERS.LOGICAL_ROUTER_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS_ROUTERS.LOGICAL_ROUTER_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS_ROUTERS.LOGICAL_ROUTER_TASK resume;