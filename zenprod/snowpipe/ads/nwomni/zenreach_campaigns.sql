-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.ADS.ZENREACH_CAMPAIGNS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.ADS.ZENREACH_CAMPAIGNS as
          select
            $1 as zenreach_campaign_id,
            $2::timestamp as start_time,
            $3::timestamp as end_time,
            $4 as status,
            $5::timestamp as created,
            $6::timestamp as updated,
            $7 as name,
            $8::integer as daily_budget_cents,
            $9 as creation_source,
            $10 as campaign_goal,
            $11::integer as total_budget_cents,
            $12::boolean as is_io_mappable, 
            $13 as io_reason,
            current_timestamp() as asof_date
          FROM @ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/view_zenreach_campaigns_snowflake.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create or replace task ZENPROD.ADS.ZENREACH_CAMPAIGNS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.ADS.ZENREACH_CAMPAIGNS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.ZENREACH_CAMPAIGNS_TASK resume;
