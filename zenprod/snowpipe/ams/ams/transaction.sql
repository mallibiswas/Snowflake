-------------------------------------------------------------------
----------------- TRANSACTION table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.TRANSACTION_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.TRANSACTION as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as account_id,
            $5::number as billing_account_id,
            $6::number as invoice_id,
            $7::number as subscription_id,
            $8 as provider_id,
            $9::timestamp as date,
            $10 as action,
            $11::number as amount_in_cents,
            $12 as status,
            $13 as salesforce_id,
            current_timestamp() as of_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/transaction.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.TRANSACTION_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.TRANSACTION_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.TRANSACTION_TASK resume;