-------------------------------------------------------------------
----------------- CONTRACT_V2 table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.CONTRACT_V2_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.CONTRACT_V2 as
          select
            $1 as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as account_id,
            $5 as contract_state,
            $6 as salesforce_oppurtinity_id,
            $7::timestamp as sent_date,
            $8::timestamp as signed_date,
            $9::number as dashboard_hardware_count,
            $10::number as dashboard_hardware_fee,
            $11::number as network_ads_setup_fee,
            $12 as network_ads_social_media_accounts,
            $13 as singer_name,
            $14 as signer_email,
            $15 as contract_hard_copy_url,
            $16 as old_world_contract_id,
            $17::number as installation_fee,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/contract_v2.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.CONTRACT_V2_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.CONTRACT_V2_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.CONTRACT_V2_TASK resume;