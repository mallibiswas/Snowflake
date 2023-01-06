-------------------------------------------------------------------
----------------- LICENSE_ASSIGNMENT table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS_PRODUCTLICENSER.LICENSE_ASSIGNMENT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS_PRODUCTLICENSER.LICENSE_ASSIGNMENT as
          select
            $1 as id,
            $2 as license_id,
            $3 as business_id,
            $4 as business_entity,
            $6::timestamp as created,
            $7::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS_PRODUCTLICENSER.AMS_PRODUCTLICENSER_S3_STAGE/${FILE_DATE}/license_assignment.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS_PRODUCTLICENSER.LICENSE_ASSIGNMENT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS_PRODUCTLICENSER.LICENSE_ASSIGNMENT_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS_PRODUCTLICENSER.LICENSE_ASSIGNMENT_TASK resume;