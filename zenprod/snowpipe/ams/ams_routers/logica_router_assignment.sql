-------------------------------------------------------------------
----------------- LOGICAL_ROUTER_ASSIGNMENT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_ROUTERS.LOGICAL_ROUTER_ASSIGNMENT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_ROUTERS.LOGICAL_ROUTER_ASSIGNMENT as
          select
            $1 as id,
            $2 as logical_router_id,
            $3 as router_assignment_id,
            $4::timestamp as assigned_in_crm,
            $5::timestamp as unassigned_in_crm,
            $6::timestamp as created,
            $7::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS_ROUTERS.AMS_ROUTERS_S3_STAGE/${FILE_DATE}/logical_router_assignment.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_ROUTERS.LOGICAL_ROUTER_ASSIGNMENT_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_ROUTERS.LOGICAL_ROUTER_ASSIGNMENT_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_ROUTERS.LOGICAL_ROUTER_ASSIGNMENT_TASK resume;