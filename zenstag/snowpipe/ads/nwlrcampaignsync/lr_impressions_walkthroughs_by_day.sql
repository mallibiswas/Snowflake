-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.ADS.LR_IMPRESSIONS_WALKTHROUGHS_BY_DAY_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.ADS.LR_IMPRESSIONS_WALKTHROUGHS_BY_DAY as
          select
            $1 as campaign_name,
            $2 as campaign_id,
            $3 as ad_group_name,
            $4 as ad_group_id,
            $5 as creative_name,
            $6 as creative_id,
            $7 as location_id,
            $8::date as sighting_day,
            $9 as aggregate7day,
            $10 as aggregate14day,
            $11 as aggregate28day,
            $12 as unique7day,
            $13 as unique14day,
            $14 as unique28day,
            current_timestamp() as asof_date
          FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwlrcampaignsync/${FILE_DATE}/view_impressions_walkthroughs_by_day.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.ADS.LR_IMPRESSIONS_WALKTHROUGHS_BY_DAY_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.ADS.LR_IMPRESSIONS_WALKTHROUGHS_BY_DAY_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.LR_IMPRESSIONS_WALKTHROUGHS_BY_DAY_TASK resume;
