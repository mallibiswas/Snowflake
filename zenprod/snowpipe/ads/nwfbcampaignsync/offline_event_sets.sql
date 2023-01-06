-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.ADS.OFFLINE_EVENT_SETS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.ADS.OFFLINE_EVENT_SETS as
          select
            $1 as offline_event_sets_id,
            $2 as best_account_id,
            split_part($3,' ',1) as business_id,
            replace(replace($3,split_part($3,' ',1)),'_',' ') as business_name,
            $4 as description,
            $5::boolean as is_auto_assign,
            $6::timestamp as creation_time,
            $7::timestamp as event_time_min,
            $8::timestamp as event_time_max,
            $9::timestamp as last_upload_time,
            parse_json($10)::variant as event_stats,
            $11::integer as valids,
            $12::integer as duplicates,
            $13::integer as matches,
            $14::integer as match_rate,
            $15 as last_synced,
            current_timestamp() as asof_date
          FROM @ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/offline_event_sets_v3.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.ADS.OFFLINE_EVENT_SETS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.ADS.OFFLINE_EVENT_SETS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.OFFLINE_EVENT_SETS_TASK resume;
