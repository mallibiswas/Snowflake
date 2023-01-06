-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.ADS.ZENREACH_CAMPAIGN_RECORDS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.ADS.ZENREACH_CAMPAIGN_RECORDS as
          select
            $1 as zenreach_campaign_records_id,
            $2 as zenreach_campaign_id,
            $3 as campaign_id,
            $4 as creation_status,
            $5 as platform,
            $6 as platform_account_id,
            $7::integer as daily_budget_cents,
            $8::integer as total_budget_cents,
            current_timestamp() as asof_date
          FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/zenreach_campaign_records.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.ADS.ZENREACH_CAMPAIGN_RECORDS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.ADS.ZENREACH_CAMPAIGN_RECORDS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.ZENREACH_CAMPAIGN_RECORDS_TASK resume;
