-------------------------------------------------------------------
----------------- PRODUCT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.PRODUCT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.PRODUCT as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4 as code,
            $5 as name,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/product.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.PRODUCT_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.PRODUCT_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.PRODUCT_TASK resume;