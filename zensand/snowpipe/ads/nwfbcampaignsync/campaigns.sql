-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.CAMPAIGNS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.CAMPAIGNS as
          select
            $1 as campaign_id,
            $2 as ad_account_id,
            $3 as name,
            $4::timestamp as start_time,
            $5::timestamp as stop_time,
            $6::timestamp as created_time,
            $7::timestamp as updated_time,
            $8 as status,
            $9 as effective_status,
            $10 as objective,
            $11::timestamp as last_synced,
            $12 as daily_spend_cents,
            $13 as lifetime_spend_cents,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/campaigns_v3.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSAND.ADS.CAMPAIGNS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.CAMPAIGNS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.CAMPAIGNS_TASK resume;
