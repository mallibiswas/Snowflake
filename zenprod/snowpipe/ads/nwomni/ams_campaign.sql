-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.ADS.AMS_CAMPAIGN_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.ADS.AMS_CAMPAIGN as
          select
            $1 as zenreach_campaign_id,
            $2 as ads_io_id,
            $3::timestamp as updated,
            $4 as mapped_by,
            current_timestamp() as asof_date
          FROM @ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/ams_campaign.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.ADS.AMS_CAMPAIGN_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.ADS.AMS_CAMPAIGN_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.AMS_CAMPAIGN_TASK resume;
