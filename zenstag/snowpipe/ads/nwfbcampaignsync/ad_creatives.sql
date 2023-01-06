-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.ADS.AD_CREATIVES_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.ADS.AD_CREATIVES as
          select
            $1 as ad_creative_id,
            $2 as ad_account_id,
            $3 as name,
            $4 as page_id,
            $5 as object_type,
            $6 as object_permalink_url,
            $7 as instagram_actor_id,
            $8 as instagram_permalink_url,
            $9 as video_id,
            $10 as status,
            $11 as title,
            $12 as body,
            $13 as thumbnail,
            $14 as image,
            $15 as call_to_action_type,
            $16 as link_description,
            $17 as link,
            $18::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/ad_creatives_v3.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.ADS.AD_CREATIVES_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.ADS.AD_CREATIVES_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.AD_CREATIVES_TASK resume;
