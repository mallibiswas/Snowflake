-------------------------------------------------------------------
----------------- SALESFORCE_QUOTE table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_ACCOUNTS.SALESFORCE_QUOTE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_ACCOUNTS.SALESFORCE_QUOTE as
          select
            $1 as id,
            $2 as salesforce_quote_id,
            $3 as billing_city,
            $4 as billing_country,
            $5 as billing_state,
            $6 as billing_street,
            $7 as billing_postal_code,
            $8 as billing_method,
            $9 as contact_id,
            $10 as contact_email,
            $11 as contact_name,
            $12 as pricebook2_id,
            $13 as oppurtunity_id,
            $14::boolean as auto_sign,
            $15 as status,
            $16 as social_media_accounts,
            $17::timestamp as created,
            $18::timestamp as updated,
            $19 as type, 
            $20 as billing_name,
            $21 as quote_to_city,
            $22 as quote_to_country,
            $23 as quote_to_state,
            $24 as quote_to_street,
            $25 as quote_to_postal_code,
            $26 as description,
            $27 as quote_to_name,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/salesforce_quote.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_ACCOUNTS.SALESFORCE_QUOTE_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_ACCOUNTS.SALESFORCE_QUOTE_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_ACCOUNTS.SALESFORCE_QUOTE_TASK resume;