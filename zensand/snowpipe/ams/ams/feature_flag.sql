-------------------------------------------------------------------
----------------- FEATURE_FLAG table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.FEATURE_FLAG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.FEATURE_FLAG as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::boolean as enabled,
            $5 as feature,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/featureflag.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.FEATURE_FLAG_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.FEATURE_FLAG_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.FEATURE_FLAG_TASK resume;