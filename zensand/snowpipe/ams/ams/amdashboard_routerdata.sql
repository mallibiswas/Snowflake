-------------------------------------------------------------------
----------------- AMDASHBOARD_ROUTERDATA table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.AMDASHBOARD_ROUTERDATA_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.AMDASHBOARD_ROUTERDATA as
          select
            $1 as business_id,
            $2::timestamp as router_last_probe_date,
            $3::timestamp as route_install_date,
            $4::timestamp as router_last_crm_request_date,
            $5::timestamp as router_install_date,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_routerdata.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.AMDASHBOARD_ROUTERDATA_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.AMDASHBOARD_ROUTERDATA_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.AMDASHBOARD_ROUTERDATA_TASK resume;