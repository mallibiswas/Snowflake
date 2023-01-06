-------------------------------------------------------------------
----------------- CUSTOMER table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.CUSTOMER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.CUSTOMER as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as account_id,
            $5::number as partner_account_id,
            $6 as first_name,
            $7 as last_name,
            $8 as email,
            $9 as phone_number,
            $10 as customer_state,
            $11 as payment_method,
            $12 as address_line1,
            $13 as address_line2,
            $14 as address_city,
            $15 as address_state,
            $16 as address_zip,
            $17 as address_country,
            $18 as external_uuid,
            $19 as info_type,
            $20::timestamp as expiry,
            $21 as holder,
            $22 as last_four,
            $23 as old_world_billing_account_id,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/customer.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.CUSTOMER_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.CUSTOMER_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.CUSTOMER_TASK resume;