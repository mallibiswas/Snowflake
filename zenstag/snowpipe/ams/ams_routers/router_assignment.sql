-------------------------------------------------------------------
----------------- ROUTER_ASSIGNMENT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS_ROUTERS.ROUTER_ASSIGNMENT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS_ROUTERS.ROUTER_ASSIGNMENT as
          select
            $1 as id,
            $2 as router_id,
            $3 as business_entity_id,
            $4::boolean as dirty,
            $5::timestamp as created,
            $6::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS_ROUTERS.AMS_ROUTERS_S3_STAGE/${FILE_DATE}/router_assignment.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS_ROUTERS.ROUTER_ASSIGNMENT_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS_ROUTERS.ROUTER_ASSIGNMENT_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS_ROUTERS.ROUTER_ASSIGNMENT_TASK resume;