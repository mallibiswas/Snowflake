create or replace procedure ZENPROD.ADS.CACHED_AD_SUMMARY_PROCEDURE()
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace table ZENPROD.ADS.CACHED_AD_SUMMARY as
          select *
          FROM ZENPROD.ADS.AD_SUMMARY;`
     }).execute();
$$;

-- Create task to call the procedure (at every 55th minute)
create or replace task ZENPROD.ADS.CACHED_AD_SUMMARY_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */55 * * * * UTC'
as 
    CALL ZENPROD.ADS.CACHED_AD_SUMMARY_PROCEDURE();

alter task ZENPROD.ADS.CACHED_AD_SUMMARY_TASK resume;
