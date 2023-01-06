-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.ADS.INSIGHTS_CUSTOM_CONVERSIONS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.ADS.INSIGHTS_CUSTOM_CONVERSIONS as
          select
            $1 as insights_custom_id,
            $2 as custom_conv_id,
            $3 as campaign_id,
            $4::integer as walkthroughs,
            $5 as insight_type,
            $6 as breakdown_type,
            $7 as breakdown_value,
            $8::timestamp as last_synced,
            $9::integer as walkthroughs1_day,
            $10::integer as walkthroughs7_day,
            $11::integer as walkthroughs28_day,
            current_timestamp() as asof_date
          FROM @ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/insights_custom_v3.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create or replace task ZENPROD.ADS.INSIGHTS_CUSTOM_CONVERSIONS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.ADS.INSIGHTS_CUSTOM_CONVERSIONS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.INSIGHTS_CUSTOM_CONVERSIONS_TASK resume;
