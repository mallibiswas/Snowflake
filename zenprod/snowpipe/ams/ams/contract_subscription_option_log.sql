-------------------------------------------------------------------
----------------- CONTRACT_SUBSCRIPTION_OPTION_LOG table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.CONTRACT_SUBSCRIPTION_OPTION_LOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.CONTRACT_SUBSCRIPTION_OPTION_LOG as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as contract_id,
            $5::number as subscription_option_id,
            $6 as original_payment_term,
            $7 as original_renewal_term,
            $8 as new_payment_term,
            $9  as discount_percent,
            $10::number as original_service_fee,
            $11::number as new_service_fee,
            $12::number as subscription_id,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/contractsubscriptionoptionlog.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.CONTRACT_SUBSCRIPTION_OPTION_LOG_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.CONTRACT_SUBSCRIPTION_OPTION_LOG_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.CONTRACT_SUBSCRIPTION_OPTION_LOG_TASK resume;