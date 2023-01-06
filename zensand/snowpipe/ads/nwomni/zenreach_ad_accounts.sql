-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.ZENREACH_AD_ACCOUNTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.ZENREACH_AD_ACCOUNTS as
          select
            $1 as ad_account_id,
            $2 as name,
            $3 as platform,
            $4 as account_id,
            $5::boolean as upload_enabled,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/ad_accounts.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.ADS.ZENREACH_AD_ACCOUNTS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.ZENREACH_AD_ACCOUNTS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.ZENREACH_AD_ACCOUNTS_TASK resume;
