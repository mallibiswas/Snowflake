-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.LR_INSIGHTS_CREATIVES_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.LR_INSIGHTS_CREATIVES as
          select
            $1 as creative_id,
            $2 as message_id,
            $3::date as date,
            $4 as device_type,
            $5 as media_type,
            $6 as impressions,
            $7 as clicks,
            $8 as cost_cents,
            $9 as impression_uniques,
            $10 as player_starts,
            $11 as player25perc_complete,
            $12 as player50perc_complete,
            $13 as player75perc_complete,
            $14 as player_completed_views,
            $15 as sampled_tracked_impressions,
            $16 as sampled_viewed_impressions,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwlrcampaignsync/${FILE_DATE}/insights_creatives.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.ADS.LR_INSIGHTS_CREATIVES_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.LR_INSIGHTS_CREATIVES_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.LR_INSIGHTS_CREATIVES_TASK resume;
