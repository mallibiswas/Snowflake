-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.ADS.INSIGHTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.ADS.INSIGHTS as
          select
            $1 as insight_id,
            $2 as ad_account_id,
            $3 as campaign_id,
            $4 as insight_type,
            $5 as breakdown_type,
            $6 as breakdown_value,
            $7::integer as impressions,
            $8::integer as clicks,
            $9::integer as walkthroughs,
            $10 as engagement,
            $11::integer as spend_cents,
            $12::timestamp as last_synced,
            $13::integer as walkthroughs1_day,
            $14::integer as walkthroughs7_day,
            $15::integer as walkthroughs28_day,
            current_timestamp() as asof_date
          FROM @ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/insights_v3.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create or replace task ZENPROD.ADS.INSIGHTS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.ADS.INSIGHTS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.INSIGHTS_TASK resume;
