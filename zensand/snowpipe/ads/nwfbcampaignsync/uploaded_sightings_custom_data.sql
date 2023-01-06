-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.UPLOADED_SIGHTINGS_CUSTOM_DATA_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.UPLOADED_SIGHTINGS_CUSTOM_DATA as
          select
            $1 as uploaded_sightings_id,
            $2 as field,
            $3 as value,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/uploaded_sightings_custom_data.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.ADS.UPLOADED_SIGHTINGS_CUSTOM_DATA_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.UPLOADED_SIGHTINGS_CUSTOM_DATA_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.UPLOADED_SIGHTINGS_CUSTOM_DATA_TASK resume;
