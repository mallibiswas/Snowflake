-------------------------------------------------------------------
----------------- ORDER_ITEM table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS_ACCOUNTS.ORDER_ITEM_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS_ACCOUNTS.ORDER_ITEM as
          select
            $1 as id,
            $2 as order_id,
            $3 as asset_id,
            $4 as salesforce_order_item_id,
            $5 as salesforce_quote_line_item_uuid,
            $6::boolean as salesforce_asset_synced,
            $7::boolean as dirty,
            $8::timestamp as created,
            $9::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/order_item.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS_ACCOUNTS.ORDER_ITEM_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS_ACCOUNTS.ORDER_ITEM_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS_ACCOUNTS.ORDER_ITEM_TASK resume;