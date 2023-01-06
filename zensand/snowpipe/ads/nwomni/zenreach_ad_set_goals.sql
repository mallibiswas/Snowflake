-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.ZENREACH_AD_SET_GOALS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.ZENREACH_AD_SET_GOALS as
          select
            $1 as zenreach_campaign_records_id,
            $2 as ad_set_id,
            $3 as goal,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/zenreach_ad_set_goals.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.ADS.ZENREACH_AD_SET_GOALS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.ZENREACH_AD_SET_GOALS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.ZENREACH_AD_SET_GOALS_TASK resume;
