-------------------------------------------------------------------
----------------- QUOTE_LINE_ITEM_INFO table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS_ACCOUNTS.QUOTE_LINE_ITEM_INFO_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS_ACCOUNTS.QUOTE_LINE_ITEM_INFO as
          select
            $1 as id,
            $2 as payment_info_id,
            $3 as salesforce_quote_line_item_id,
            $4::timestamp as created,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/quote_line_item_info.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS_ACCOUNTS.QUOTE_LINE_ITEM_INFO_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS_ACCOUNTS.QUOTE_LINE_ITEM_INFO_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS_ACCOUNTS.QUOTE_LINE_ITEM_INFO_TASK resume;