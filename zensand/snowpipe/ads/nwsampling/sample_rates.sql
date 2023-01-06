-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.SAMPLE_RATES_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.SAMPLE_RATES as
          select
            $1 as business_id, 
            $2::number as sample_rate_multiplier, 
            $3::date as day, 
            $4::timestamp_ntz as updated, 
            $5 as source, 
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwsampling/${FILE_DATE}/sample_rates.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.ADS.SAMPLE_RATES_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.SAMPLE_RATES_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.SAMPLE_RATES_TASK resume;
