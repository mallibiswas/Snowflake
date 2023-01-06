-------------------------------------------------------------------
----------------- NETWORK_ASSIGNMENT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS_ROUTERS.NETWORK_ASSIGNMENT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS_ROUTERS.NETWORK_ASSIGNMENT as
          select
            $1 as id,
            $2 as business_entity_id,
            $3 as cloud_network_id,
            $4 as meraki_network_id,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS_ROUTERS.AMS_ROUTERS_S3_STAGE/${FILE_DATE}/network_assignment.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS_ROUTERS.NETWORK_ASSIGNMENT_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS_ROUTERS.NETWORK_ASSIGNMENT_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS_ROUTERS.NETWORK_ASSIGNMENT_TASK resume;