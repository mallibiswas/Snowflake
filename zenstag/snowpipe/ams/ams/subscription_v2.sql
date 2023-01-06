-------------------------------------------------------------------
----------------- SUBSCRIPTION_V2 table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.SUBSCRIPTIONS_V2_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.SUBSCRIPTIONS_V2 as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as account_id,
            $5::number as billing_account_id,
            $6::number as plan_id,
            $7::number as previous_subscription_id,
            $8::number as contract_id,
            $9 as subscription_state,
            $10::number as service_fee,
            $11::timestamp as subscription_start_date,
            $12::number as trial_length,
            $13::number as number_of_billing_cycles,
            $14::number as wait_period,
            $15 as recurly_subscription_id,
            $16 as sales_rep,
            $17 as notes,
            $18 as old_world_subscription_id,
            $19 as salesforce_oppurtunity_id,
            $20 as salesforce_id,
            $21::number as renewal_term_months,
            $22::number as package_id,
            $23 as recurly_coupon_id,
            $24::timestamp as absolute_trial_end_date,
            $25::timestamp as contract_expiry_pivot,
            $26::timestamp as payment_date_pivot,
            current_timestamp() as of_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/subscriptions_v2.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.SUBSCRIPTIONS_V2_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.SUBSCRIPTIONS_V2_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.SUBSCRIPTIONS_V2_TASK resume;