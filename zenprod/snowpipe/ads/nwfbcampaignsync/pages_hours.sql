-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.ADS.PAGES_HOURS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.ADS.PAGES_HOURS as
          select
            $1 as pages_hour_id,
            $2 as page_id,
            $3 as hour_key,
            $4 as hour_value,
            $5::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM @ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/pages_hours_v3.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.ADS.PAGES_HOURS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.ADS.PAGES_HOURS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.PAGES_HOURS_TASK resume;
