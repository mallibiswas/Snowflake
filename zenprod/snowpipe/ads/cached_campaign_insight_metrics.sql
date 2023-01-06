create or replace procedure ZENPROD.ADS.CACHED_CAMPAIGN_INSIGHT_METRICS_PROCEDURE()
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace table ZENPROD.ADS.CACHED_CAMPAIGN_INSIGHT_METRICS as
          select *
          FROM ZENPROD.ADS.CAMPAIGN_INSIGHT_METRICS;`
     }).execute();
$$;

-- Create task to call the procedure (at every 55th minute)
create or replace task ZENPROD.ADS.CACHED_CAMPAIGN_INSIGHT_METRICS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */55 * * * * UTC'
as 
    CALL ZENPROD.ADS.CACHED_CAMPAIGN_INSIGHT_METRICS_PROCEDURE();

alter task ZENPROD.ADS.CACHED_CAMPAIGN_INSIGHT_METRICS_TASK resume;
