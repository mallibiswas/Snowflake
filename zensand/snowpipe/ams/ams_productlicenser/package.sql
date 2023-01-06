-------------------------------------------------------------------
----------------- PACKAGE table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS_PRODUCTLICENSER.PACKAGE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS_PRODUCTLICENSER.PACKAGE as
          select
            $1 as id,
            $2 as product_id,
            $3 as name,
            $4 as code,
            $5::timestamp as created,
            $6::timestamp as updated,
            $7::number as monthly_list_price,
            current_timestamp() as of_date
          FROM @ZENSAND.AMS_PRODUCTLICENSER.AMS_PRODUCTLICENSER_S3_STAGE/${FILE_DATE}/package.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS_PRODUCTLICENSER.PACKAGE_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS_PRODUCTLICENSER.PACKAGE_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS_PRODUCTLICENSER.PACKAGE_TASK resume;