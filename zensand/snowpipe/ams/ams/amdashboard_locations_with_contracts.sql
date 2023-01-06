-------------------------------------------------------------------
----------------- AMDASHBOARD_LOCATIONS_WITH_CONTRACTS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.AMDASHBOARD_LOCATIONS_WITH_CONTRACTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.AMDASHBOARD_LOCATIONS_WITH_CONTRACTS as
          select
            $1 as business_id,
            $2 as ams_account_id,
            $3 as account_name,
            $4::timestamp as effective_date,
            $5::timestamp as expiration_date,
            $6::number as hardware_fee,
            $7 as payment_method,
            $8 as payment_term,
            $9::number as pilot_extension,
            $10::number as pilot_length,
            $11 as renewal_term,
            $12 as sales_rep,
            $13::number as service_fee,
            $14::timestamp as signed,
            $15::number as contract_id,
            $16::number as location_id,
            $17 as location_state,
            $18 as name,
            $19 as salesforce_id,
            $20 as signer_email,
            $21::float as latitude,
            $22::float as longitude,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/amdashboard_locations_with_contracts.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.AMDASHBOARD_LOCATIONS_WITH_CONTRACTS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.AMDASHBOARD_LOCATIONS_WITH_CONTRACTS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.AMDASHBOARD_LOCATIONS_WITH_CONTRACTS_TASK resume;