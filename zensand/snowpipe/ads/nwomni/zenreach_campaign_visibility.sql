-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.ZENREACH_CAMPAIGN_VISIBILITY_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.ZENREACH_CAMPAIGN_VISIBILITY as
          select
            $1 as zenreach_campaign_id,
            $2::boolean as is_visible_on_dashboard,
            $3 as fb_display_attribution_window,
            $4::text as lr_display_attribution_window,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/zenreach_campaign_visibility.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSAND.ADS.ZENREACH_CAMPAIGN_VISIBILITY_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.ZENREACH_CAMPAIGN_VISIBILITY_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.ZENREACH_CAMPAIGN_VISIBILITY_TASK resume;
