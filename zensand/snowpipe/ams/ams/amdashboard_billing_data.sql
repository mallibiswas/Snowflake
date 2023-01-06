-------------------------------------------------------------------
----------------- AMDASHBOARD_BILLING_DATA table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.AMDASHBOARD_BILLING_DATA_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.AMDASHBOARD_BILLING_DATA as
          select
            $1 as business_id,
            $2 as account_id,
            $3::timestamp as next_billing_date,
            $4::number as invoice_past_due_count,
            $5::timestamp as invoice_past_due_first_date,
            $6::number as invoice_past_due_amount,
            $7::number as invoice_paid_count,
            $8::timestamp as invoice_paid_first_date,
            $9::number as invoice_paid_amount,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_billing_data.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.AMDASHBOARD_BILLING_DATA_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.AMDASHBOARD_BILLING_DATA_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.AMDASHBOARD_BILLING_DATA_TASK resume;



