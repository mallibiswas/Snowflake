-------------------------------------------------------------------
----------------- SALESFORCE_ASSET table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS_ACCOUNTS.SALESFORCE_ASSET_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS_ACCOUNTS.SALESFORCE_ASSET as
          select
            $1 as id,
            $2 as parent_id,
            $3::number as quantity,
            $4::number as unit_price_cents,
            $5::timestamp as installed_date,
            $6::timestamp as purchase_date,
            $7::timestamp as termination_date,
            $8::timestamp as created,
            $9::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/salesforce_asset.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS_ACCOUNTS.SALESFORCE_ASSET_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS_ACCOUNTS.SALESFORCE_ASSET_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS_ACCOUNTS.SALESFORCE_ASSET_TASK resume;