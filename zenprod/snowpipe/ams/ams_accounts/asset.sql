-------------------------------------------------------------------
----------------- ASSET table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_ACCOUNTS.ASSET_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_ACCOUNTS.ASSET as
          select
            $1 as id,
            $2 as account_id,
            $3 as salesforce_asset_id,
            $4 as subscription_id,
            $5 as charge_id,
            $6 as item_type,
            $7::timestamp as created,
            $8::timestamp as updated,
            $9 as payment_info_id,
            $10 as campaign_id,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/asset.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_ACCOUNTS.ASSET_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_ACCOUNTS.ASSET_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_ACCOUNTS.ASSET_TASK resume;