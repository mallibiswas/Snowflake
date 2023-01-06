-------------------------------------------------------------------
----------------- CONTRACT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.CONTRACT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.CONTRACT as
          select
            $1 as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as account_id,
            $5::number as subscription_id,
            $6::number as billing_account_id,
            $7 as salesforce_id,
            $8::number as service_fee,
            $9::number as hardware_fee,
            $10::number as num_aps,
            $11 as payment_term,
            $12 as payment_method,
            $13 as singer_name,
            $14 as signer_email,
            $15::number as pilot_length,
            $16 as pilot_type,
            $17::number as pilot_extension,
            $18::timestamp as effective_date,
            $19::timestamp as expiration_date,
            $20 as renewal_term,
            $21::timestamp as sent,
            $22::timestamp as signed,
            $23 as contract_state,
            $24::boolean as beta,
            $25 as sales_rep,
            $26::number as wait_period,
            $27::number as category_id,
            $28 as business_unit,
            $29::boolean as use_ams_contract,
            $30::boolean as use_ams_billing,
            $31 as agreement_version,
            $32 as salesforce_oppurtinity_id,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/contract.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.CONTRACT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.CONTRACT_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.CONTRACT_TASK resume;