-------------------------------------------------------------------
----------------- CONTRACT_CATEGORY table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.CONTRACT_CATEGORY_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.CONTRACT_CATEGORY as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4 as title,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/contractcategory.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.CONTRACT_CATEGORY_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.CONTRACT_CATEGORY_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.CONTRACT_CATEGORY_TASK resume;