-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.ADS.AD_SETS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.ADS.AD_SETS as
          select
            $1 as ad_set_id,
            $2 as ad_account_id,
            $3 as campaign_id,
            $4 as name,
            $5::timestamp as start_time,
            $6::timestamp as stop_time,
            $7::timestamp as created_time,
            $8::timestamp as updated_time,
            $9 as status,
            $10 as effective_status,
            $11 as optimization_goal,
            $12 as billing_event,
            $13 as bid_strategy,
            $14 as daily_budget,
            $15 as budget_remaining,
            $16 as age_min,
            $17 as age_max,
            $18::boolean as has_facebook,
            $19::boolean as has_instagram,
            $20::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/ad_sets_v3.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.ADS.AD_SETS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.ADS.AD_SETS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.AD_SETS_TASK resume;
