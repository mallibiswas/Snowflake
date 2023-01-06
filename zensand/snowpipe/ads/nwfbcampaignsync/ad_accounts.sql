-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.AD_ACCOUNTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.AD_ACCOUNTS as
          select
            $1 as ad_account_id,
            $2 as name,
            $3::boolean as is_zenreach,
            $4::timestamp as created_time,
            $5::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/ad_accounts_v3.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.ADS.AD_ACCOUNTS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.AD_ACCOUNTS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.AD_ACCOUNTS_TASK resume;
