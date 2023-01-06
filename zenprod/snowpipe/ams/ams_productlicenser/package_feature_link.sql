-------------------------------------------------------------------
----------------- PACKAGE_FEATURE_LINK table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_PRODUCTLICENSER.PACKAGE_FEATURE_LINK_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_PRODUCTLICENSER.PACKAGE_FEATURE_LINK as
          select
            $1 as id,
            $2 as package_id,
            $3 as feature_id,
            $4::timestamp as created,
            $5::timestamp as updated,
            current_timestamp() as of_date
          FROM @ZENPROD.AMS_PRODUCTLICENSER.AMS_PRODUCTLICENSER_S3_STAGE/${FILE_DATE}/package_feature_link.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_PRODUCTLICENSER.PACKAGE_FEATURE_LINK_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_PRODUCTLICENSER.PACKAGE_FEATURE_LINK_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_PRODUCTLICENSER.PACKAGE_FEATURE_LINK_TASK resume;
