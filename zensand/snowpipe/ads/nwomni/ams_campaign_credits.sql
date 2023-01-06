-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.AMS_CAMPAIGN_CREDITS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.AMS_CAMPAIGN_CREDITS as
          select
            $1 as ams_campaign_credits_id,
            $2 as ams_campaign_id,
            $3 as reason,
            $4 as value_type,
            $5::float as value,
            $6::timestamp as start_date,
            $7::timestamp as end_date,
            $8 as notes,
            $9 as username,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/ams_campaign_credits.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSAND.ADS.AMS_CAMPAIGN_CREDITS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.AMS_CAMPAIGN_CREDITS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.AMS_CAMPAIGN_CREDITS_TASK resume;
