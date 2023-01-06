-------------------------------------------------------------------
----------------- PACKAGE_FEATURE_THROUGH table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.PACKAGE_FEATURE_THROUGH_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.PACKAGE_FEATURE_THROUGH as
          select
            $1::number as id,
            $2::number as package_id,
            $3::number as feature_id,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/package_feature_through.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.PACKAGE_FEATURE_THROUGH_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.PACKAGE_FEATURE_THROUGH_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.PACKAGE_FEATURE_THROUGH_TASK resume;