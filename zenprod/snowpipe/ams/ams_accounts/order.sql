-------------------------------------------------------------------
----------------- ORDER table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_ACCOUNTS.ORDER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_ACCOUNTS.ORDERS as
          select
            $1 as id,
            $2 as account_id,
            $3 as salesforce_quote_uuid,
            $4 as salesforce_order_id,
            $5 as signer_name,
            $6::timestamp as signed_date,
            $7 as hardcopy_url,
            $8::timestamp as created,
            $9::timestamp as updated,
            $10::timestamp as cancelled,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/order.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_ACCOUNTS.ORDER_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_ACCOUNTS.ORDER_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_ACCOUNTS.ORDER_TASK resume;