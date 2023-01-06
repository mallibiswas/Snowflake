-------------------------------------------------------------------
----------------- SALESFORCE_QUOTE_LINE_ITEM table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS_ACCOUNTS.SALESFORCE_QUOTE_LINE_ITEM_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS_ACCOUNTS.SALESFORCE_QUOTE_LINE_ITEM as
          select
            $1 as id,
            $2 as salesforce_quote_uuid,
            $3 as salesforce_quote_line_item_id,
            $4 as asset_id,
            $5 as replacement_quote_line_id,
            $6 as product2_id,
            $7 as product2_name,
            $8 as product2_code,
            $9 as product_family,
            $10 as product_sku,
            $11 as product_category,
            $12::boolean as invoice_now,
            $13::number as billing_frequency_months,
            $14 as pricebook_entry_id,
            $15::number as quantity,
            $16::number as unit_price_cents,
            $17::number as discount_percent,
            $18::number as total_price_cents,
            $19::timestamp as start_date,
            $20::timestamp as end_date,
            $21::timestamp as created,
            $22::timestamp as updated,
            $23::number as margin_percent,
            $24 as description,
            $25 as internal_description,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/salesforce_quote_line_item.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS_ACCOUNTS.SALESFORCE_QUOTE_LINE_ITEM_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS_ACCOUNTS.SALESFORCE_QUOTE_LINE_ITEM_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS_ACCOUNTS.SALESFORCE_QUOTE_LINE_ITEM_TASK resume;