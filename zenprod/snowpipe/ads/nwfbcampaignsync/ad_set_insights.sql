-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.ADS.AD_SET_INSIGHTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.ADS.AD_SET_INSIGHTS as
          select
            $1 as insight_id,
            $2 as ad_account_id,
            $3 as campaign_id,
            $4 as ad_set_id,
            $5 as insight_type,
            $6 as breakdown_type,
            $7 as breakdown_value,
            $8::integer as impressions,
            $9::integer as clicks,
            $10::integer as walkthroughs,
            $11 as engagement,
            $12::integer as spend_cents,
            $13::integer as thru_plays,
            $14::integer as video_plays_at_25_perc,
            $15::integer as video_plays_at_50_perc,
            $16::integer as video_plays_at_75_perc,
            $17::integer as video_plays_at_95_perc,
            $18::integer as video_plays_at_100_perc,
            $19::integer as video_plays,
            $20::integer as video_average_play_time,
            $21::integer as instant_experience_view_time,
            $22::integer as instant_experience_view_perc,
            $23::integer as link_clicks,
            $24::integer as outbound_clicks,
            $25::integer as instant_experience_clicks_to_open,
            $26::integer as instant_experience_clicks_to_start,
            $27::integer as instant_experience_outbound_clicks,
            $28::integer as e_cpm_cents,
            $29::timestamp as last_synced,
            $30::integer as walkthroughs1_day,
            $31::integer as walkthroughs7_day,
            $32::integer as walkthroughs28_day,
            current_timestamp() as asof_date
          FROM @ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/insights_ad_sets_v3.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create or replace task ZENPROD.ADS.AD_SET_INSIGHTS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.ADS.AD_SET_INSIGHTS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.AD_SET_INSIGHTS_TASK resume;
