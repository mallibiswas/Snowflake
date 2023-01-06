-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.ADS.ZENREACH_CAMPAIGN_RECORDS_LOCATIONS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.ADS.ZENREACH_CAMPAIGN_RECORDS_LOCATIONS as
          select
            $1 as zenreach_campaign_records_id,
            $2 as location_id,
            current_timestamp() as asof_date
          FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/zenreach_campaign_records_locations.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.ADS.ZENREACH_CAMPAIGN_RECORDS_LOCATIONS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.ADS.ZENREACH_CAMPAIGN_RECORDS_LOCATIONS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.ZENREACH_CAMPAIGN_RECORDS_LOCATIONS_TASK resume;
