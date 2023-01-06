-------------------------------------------------------------------
----------------- BILLINGACCOUNT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.BILLINGACCOUNT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.BILLING_ACCOUNT as
          select
            $1 as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as account_id,
            $5 as provider_id,
            $6 as bc_state,
            $7 as name,
            $8 as payment_type,
            $9 as last_four,
            $10 as info_type,
            $11 as holder,
            $12::timestamp as expiry,
            $13 as address_line1,
            $14 as address_line2,
            $15 as address_zip,
            $16 as address_city,
            $17 as address_state,
            $18 as address_county,
            $19::number as partner_account_id,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/billingaccount.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.BILLINGACCOUNT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.BILLINGACCOUNT_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.BILLINGACCOUNT_TASK resume;