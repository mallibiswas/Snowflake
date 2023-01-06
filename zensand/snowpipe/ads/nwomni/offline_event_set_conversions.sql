-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.OFFLINE_EVENT_SET_CONVERSIONS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.OFFLINE_EVENT_SET_CONVERSIONS as
          select
            $1 as offline_event_set_id,
            $2 as location_id,
            $3 as custom_conversion_id,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/offline_event_set_conversions.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.ADS.OFFLINE_EVENT_SET_CONVERSIONS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.OFFLINE_EVENT_SET_CONVERSIONS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.OFFLINE_EVENT_SET_CONVERSIONS_TASK resume;
