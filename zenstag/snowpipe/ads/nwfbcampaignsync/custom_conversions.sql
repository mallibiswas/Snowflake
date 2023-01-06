-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.ADS.CUSTOM_CONVERSIONS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.ADS.CUSTOM_CONVERSIONS as
          select
            $1 as custom_conversion_id,
            $2 as account_id,
            $3 as custom_event_type,
            $4 as name,
            $5::timestamp as last_synced,
            $6::boolean as is_archived,
            $7 as offline_event_set_id,
            current_timestamp() as asof_date
          FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/view_custom_conversions_snowflake.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.ADS.CUSTOM_CONVERSIONS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.ADS.CUSTOM_CONVERSIONS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.CUSTOM_CONVERSIONS_TASK resume;
