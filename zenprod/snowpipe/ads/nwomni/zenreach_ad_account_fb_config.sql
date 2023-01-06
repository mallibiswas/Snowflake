-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.ADS.ZENREACH_AD_ACCOUNT_FB_CONFIG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.ADS.ZENREACH_AD_ACCOUNT_FB_CONFIG as
          select
            $1 as ad_account_id,
            $2 as facebook_ad_account_id,
            $3 as offline_event_set_id,
            $4 as facebook_page_id,
            $5 as default_attribution_window,
            current_timestamp() as asof_date
          FROM @ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/ad_account_fb_config.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create or replace task ZENPROD.ADS.ZENREACH_AD_ACCOUNT_FB_CONFIG_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.ADS.ZENREACH_AD_ACCOUNT_FB_CONFIG_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.ZENREACH_AD_ACCOUNT_FB_CONFIG_TASK resume;
