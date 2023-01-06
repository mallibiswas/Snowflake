-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.LR_CREATIVES_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.LR_CREATIVES as
          select
            $1 as id,
            $2 as creative_id,
            $3 as ad_group_id,
            $4 as name,
            $5::timestamp as created,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwlrcampaignsync/${FILE_DATE}/creatives.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.ADS.LR_CREATIVES_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.LR_CREATIVES_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.LR_CREATIVES_TASK resume;
