-------------------------------------------------------------------
----------------- FEATURE_FLAG table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.FEATURE_FLAG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.FEATURE_FLAG as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::boolean as enabled,
            $5 as feature,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/featureflag.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.FEATURE_FLAG_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.FEATURE_FLAG_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.FEATURE_FLAG_TASK resume;