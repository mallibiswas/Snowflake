-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.UPLOADED_SIGHTINGS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.UPLOADED_SIGHTINGS as
          select
            $1 as uploaded_sightings_id,
            $2 as sighting_id,
            $3 as offline_event_set_id,
            $4 as business_id,
            $5::timestamp as end_time,
            $6::timestamp as uploaded,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/uploaded_sightings.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.ADS.UPLOADED_SIGHTINGS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.UPLOADED_SIGHTINGS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.UPLOADED_SIGHTINGS_TASK resume;
