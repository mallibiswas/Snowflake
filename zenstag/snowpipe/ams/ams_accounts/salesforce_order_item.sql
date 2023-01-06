-------------------------------------------------------------------
----------------- SALESFORCE_ORDER_ITEM table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS_ACCOUNTS.SALESFORCE_ORDER_ITEM_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS_ACCOUNTS.SALESFORCE_ORDER_ITEM as
          select
            $1 as id,
            $2 as replacement_order_item_id,
            $3::number as quantity,
            $4::timestamp as created,
            $5::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/salesforce_order_item.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS_ACCOUNTS.SALESFORCE_ORDER_ITEM_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS_ACCOUNTS.SALESFORCE_ORDER_ITEM_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS_ACCOUNTS.SALESFORCE_ORDER_ITEM_TASK resume;