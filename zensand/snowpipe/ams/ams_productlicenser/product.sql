-------------------------------------------------------------------
----------------- PRODUCT table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS_PRODUCTLICENSER.PRODUCT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS_PRODUCTLICENSER.PRODUCT as
          select
            $1 as id,
            $2 as name,
            $3 as code,
            $4::timestamp as created,
            $5::timestamp as updated,
            current_timestamp() as of_date
          FROM @ZENSAND.AMS_PRODUCTLICENSER.AMS_PRODUCTLICENSER_S3_STAGE/${FILE_DATE}/product.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS_PRODUCTLICENSER.PRODUCT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS_PRODUCTLICENSER.PRODUCT_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS_PRODUCTLICENSER.PRODUCT_TASK resume;