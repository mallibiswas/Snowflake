-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.ADS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.ADS as
          select
            $1 as ad_id,
            $2 as ad_account_id,
            $3 as campaign_id,
            $4 as ad_set_id,
            $5 as ad_creative_id,
            $6 as name,
            $7::timestamp as created_time,
            $8::timestamp as updated_time,
            $9 as status,
            $10 as effective_status,
            $11::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/ads_v3.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.ADS.ADS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.ADS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.ADS_TASK resume;
