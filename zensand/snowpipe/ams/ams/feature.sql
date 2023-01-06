-------------------------------------------------------------------
----------------- FEATURE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.FEATURE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.FEATURE as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4 as code,
            $5 as name,
            $6 as description,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/feature.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.FEATURE_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.FEATURE_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.FEATURE_TASK resume;