-------------------------------------------------------------------
----------------- DISCOUNT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.DISCOUNT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.DISCOUNT as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4 as name,
            $5 as description,
            $6 as coupon_code,
            $7 as condition,
            $8::boolean as is_active,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/discount.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.DISCOUNT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.DISCOUNT_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.DISCOUNT_TASK resume;