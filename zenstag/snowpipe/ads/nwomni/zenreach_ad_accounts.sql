-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.ADS.ZENREACH_AD_ACCOUNTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.ADS.ZENREACH_AD_ACCOUNTS as
          select
            $1 as ad_account_id,
            $2 as name,
            $3 as platform,
            $4 as account_id,
            $5::boolean as upload_enabled,
            current_timestamp() as asof_date
          FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/ad_accounts.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.ADS.ZENREACH_AD_ACCOUNTS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.ADS.ZENREACH_AD_ACCOUNTS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.ZENREACH_AD_ACCOUNTS_TASK resume;
