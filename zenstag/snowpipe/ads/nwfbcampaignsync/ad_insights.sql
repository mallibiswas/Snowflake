-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.ADS.AD_INSIGHTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.ADS.AD_INSIGHTS as
          select
            $1 as insight_id,
            $2 as ad_account_id,
            $3 as campaign_id,
            $4 as ad_set_id,
            $5 as ad_id,
            $6 as insight_type,
            $7 as breakdown_type,
            $8 as breakdown_value,
            $9::integer as impressions,
            $10::integer as clicks,
            $11::integer as walkthroughs,
            $12 as engagement,
            $13::integer as spend_cents,
            $14::integer as thru_plays,
            $15::integer as video_plays_at_25_perc,
            $16::integer as video_plays_at_50_perc,
            $17::integer as video_plays_at_75_perc,
            $18::integer as video_plays_at_95_perc,
            $19::integer as video_plays_at_100_perc,
            $20::integer as video_plays,
            $21::integer as video_average_play_time,
            $22::integer as instant_experience_view_time,
            $23::integer as instant_experience_view_perc,
            $24::integer as link_clicks,
            $25::integer as outbound_clicks,
            $26::integer as instant_experience_clicks_to_open,
            $27::integer as instant_experience_clicks_to_start,
            $28::integer as instant_experience_outbound_clicks,
            $29::integer as e_cpm_cents,
            $30::timestamp as last_synced,
            $31::integer as walkthroughs1_day,
            $32::integer as walkthroughs7_day,
            $33::integer as walkthroughs28_day,
            current_timestamp() as asof_date
          FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/insights_ads_v3.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSTAG.ADS.AD_INSIGHTS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.ADS.AD_INSIGHTS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.AD_INSIGHTS_TASK resume;
