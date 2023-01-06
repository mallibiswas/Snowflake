-------------------------------------------------------------------
----------------- INVOICE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.INVOICE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.INVOICE as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as account_id,
            $5::number as subscription_id,
            $6::number as billing_account_id,
            $7::number as provider_id,
            $8::timestamp as date,
            $9::number as invoice_number,
            $10 as invoice_state,
            $11::number as total_in_cents,
            $12 as currency,
            $13::timestamp as closed_at,
            $14 as collection_method,
            $15 as net_terms,
            $16 as salesforce_id,
            $17::boolean as processed_for_referral,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/invoice.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.INVOICE_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.INVOICE_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.INVOICE_TASK resume;