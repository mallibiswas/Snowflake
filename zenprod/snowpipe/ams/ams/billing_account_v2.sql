-------------------------------------------------------------------
----------------- BILLING_ACCOUNT_V2 table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.BILLING_ACCOUNT_V2_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.BILLING_ACCOUNT_V2 as
          select
            $1 as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as account_id,
            $5::number as partner_account_id,
            $6 as email,
            $7 as phone_number,
            $8 as billing_account_state,
            $9 as payment_method,
            $10 as address_line1,
            $11 as address_line2,
            $12 as address_city,
            $13 as address_state,
            $14 as address_zip,
            $15 as address_country,
            $16 as recurly_account_id,
            $17 as info_type,
            $18::timestamp as expiry,
            $19 as holder,
            $20 as last_four,
            $21 as old_world_billing_account_id,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/billing_account_v2.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.BILLING_ACCOUNT_V2_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.BILLING_ACCOUNT_V2_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.BILLING_ACCOUNT_V2_TASK resume;