-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.ZENREACH_CAMPAIGN_RECORDS_MARGINS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.ZENREACH_CAMPAIGN_RECORDS_MARGINS as
          select
            $1 as campaign_id,
            $2::float as margin_percent,
            $3::timestamp as updated,
            $4 as zenreach_campaign_records_id,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/view_zenreach_campaign_records_margins.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.ADS.ZENREACH_CAMPAIGN_RECORDS_MARGINS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.ZENREACH_CAMPAIGN_RECORDS_MARGINS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.ZENREACH_CAMPAIGN_RECORDS_MARGINS_TASK resume;
