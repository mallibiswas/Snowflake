-------------------------------------------------------------------
----------------- FEATURE table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS_PRODUCTLICENSER.FEATURE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS_PRODUCTLICENSER.FEATURE as
          select
            $1 as id,
            $2 as name,
            $3 as code,
            $4::timestamp as created,
            $5::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS_PRODUCTLICENSER.AMS_PRODUCTLICENSER_S3_STAGE/${FILE_DATE}/feature.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS_PRODUCTLICENSER.FEATURE_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS_PRODUCTLICENSER.FEATURE_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS_PRODUCTLICENSER.FEATURE_TASK resume;
