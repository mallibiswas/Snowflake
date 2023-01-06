-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.ADS.UPLOADED_SIGHTINGS_LIVERAMP_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.ADS.UPLOADED_SIGHTINGS_LIVERAMP as
          select
            $1 as uploaded_sightings_id,
            $2 as sighting_id,
            $3 as business_id,
            $4::timestamp as end_time,
            current_timestamp() as asof_date
          FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/liveramp_measurement/${FILE_DATE}/uploaded_sightings.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.ADS.UPLOADED_SIGHTINGS_LIVERAMP_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.ADS.UPLOADED_SIGHTINGS_LIVERAMP_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.UPLOADED_SIGHTINGS_LIVERAMP_TASK resume;
