-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.ADS.LR_CAMPAIGNS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.ADS.LR_CAMPAIGNS as
          select
            $1 as id,
            $2 as campaign_id,
            $3 as advertiser_id,
            $4 as name,
            $5::timestamp as created,
            current_timestamp() as asof_date
          FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwlrcampaignsync/${FILE_DATE}/campaigns.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.ADS.LR_CAMPAIGNS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.ADS.LR_CAMPAIGNS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.LR_CAMPAIGNS_TASK resume;
