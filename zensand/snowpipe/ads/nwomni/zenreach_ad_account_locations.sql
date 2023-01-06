-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.ZENREACH_AD_ACCOUNT_LOCATIONS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.ZENREACH_AD_ACCOUNT_LOCATIONS as
          select
            $1 as ad_account_id,
            $2 as location_id,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/ad_account_locations.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.ADS.ZENREACH_AD_ACCOUNT_LOCATIONS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.ZENREACH_AD_ACCOUNT_LOCATIONS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.ZENREACH_AD_ACCOUNT_LOCATIONS_TASK resume;
