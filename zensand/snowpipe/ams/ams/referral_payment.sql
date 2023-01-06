-------------------------------------------------------------------
----------------- REFERRAL_PAYMENT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.REFERRAL_PAYMENT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.REFERRAL_PAYMENT as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as referrer_id,
            $5::number as location_id,
            $6::number as source_invoice_id,
            $7::number as amount_in_cents,
            $8::number(18,12) as percentage,
            $9::number as payout_bill_id,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/referralpayment.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.REFERRAL_PAYMENT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.REFERRAL_PAYMENT_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.REFERRAL_PAYMENT_TASK resume;